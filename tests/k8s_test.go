package tests

import (
	"build-to-deploy/internal/k8s"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"build-to-deploy/internal/sshpool"
)

type fakeR struct {
	cmds   []string
	output string
	err    error
	runErr error
}

func (f *fakeR) RunCommand(ctx context.Context, srv *sshpool.ServerConfig, cmd string, w io.Writer) error {
	f.cmds = append(f.cmds, cmd)
	return f.runErr
}
func (f *fakeR) MarkSimulated(*sshpool.ServerConfig) {}

func (f *fakeR) ExecuteOutput(ctx context.Context, srv *sshpool.ServerConfig, cmd string) (string, error) {
	f.cmds = append(f.cmds, cmd)
	return f.output, f.err
}

var srv = &sshpool.ServerConfig{Host: "h", Port: 22, User: "root"}

func TestK8sCheckName(t *testing.T) {
	if err := k8s.CheckName("命名空间", "shuili"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "Upper", "a b", "-lead", strings.Repeat("a", 64)} {
		if err := k8s.CheckName("命名空间", bad); err == nil {
			t.Fatalf("%q 应校验失败", bad)
		}
	}
}

// 每个 K8s 操作都必须真实下发 kubectl 命令：没有任何模拟分支
func TestK8sAllOperationsHitRemote(t *testing.T) {
	f := &fakeR{}
	c := k8s.NewClient(f)
	ctx := context.Background()
	if _, err := c.ListPods(ctx, srv, "BAD"); err == nil {
		t.Fatal("应校验命名空间")
	}
	if err := c.SetImage(ctx, &bytes.Buffer{}, srv, "shuili", "demo", "demo", "bad image"); err == nil {
		t.Fatal("非法镜像应报错")
	}
	f.output = `{"items":[]}`
	for _, call := range []func() error{
		func() error { _, err := c.ListNamespaces(ctx, srv); return err },
		func() error { _, err := c.ListPods(ctx, srv, "shuili"); return err },
		func() error { _, err := c.ListPods(ctx, srv, k8s.AllNamespaces); return err },
		func() error { _, err := c.GetPodLogs(ctx, srv, "shuili", "pod-1", "", 10); return err },
		func() error { return c.RolloutStatus(ctx, &bytes.Buffer{}, srv, "shuili", "demo", 1) },
		func() error { return c.RestartDeployment(ctx, srv, "shuili", "demo") },
		func() error {
			return c.SetImage(ctx, &bytes.Buffer{}, srv, "shuili", "demo", "demo", "app/demo:v1")
		},
	} {
		if err := call(); err != nil {
			t.Fatalf("应真实执行: %v", err)
		}
	}
	if len(f.cmds) != 7 {
		t.Fatalf("应下发 7 条远程命令，实际 %d: %v", len(f.cmds), f.cmds)
	}
}

func TestK8sListPodsAllNamespaces(t *testing.T) {
	f := &fakeR{output: `{"items":[{"metadata":{"name":"p1","namespace":"a"},"spec":{"containers":[{"name":"c1","image":"app:v1"}]},"status":{"phase":"Running"}},{"metadata":{"name":"p2","namespace":"b"},"spec":{"containers":[{"name":"c1","image":"app:v1"}]},"status":{"phase":"Running"}}]}`}
	c := k8s.NewClient(f)
	pods, err := c.ListPods(context.Background(), srv, k8s.AllNamespaces)
	if err != nil || len(pods) != 2 {
		t.Fatalf("全部命名空间查询失败: %v %v", pods, err)
	}
	if f.cmds[len(f.cmds)-1] != "kubectl get pods -A -o json" {
		t.Fatalf("应执行全命名空间查询，实际: %s", f.cmds[len(f.cmds)-1])
	}
	if pods[0].Namespace != "a" || pods[1].Namespace != "b" {
		t.Fatalf("应保留 Pod 真实命名空间: %+v", pods)
	}
}

func TestK8sRealPathsWithFakeRunner(t *testing.T) {
	f := &fakeR{}
	c := k8s.NewClient(f)
	ctx := context.Background()
	c.ListNamespaces(ctx, srv)
	if !strings.Contains(f.cmds[0], "kubectl get namespaces") {
		t.Fatalf("命令 = %s", f.cmds[0])
	}

	// 空输出时默认返回 default
	f.output = "  \n"
	if ns, _ := c.ListNamespaces(ctx, srv); len(ns) != 1 || ns[0] != "default" {
		t.Fatalf("空输出应返回 default: %v", ns)
	}
	f.err = errors.New("ssh down")
	if _, err := c.ListNamespaces(ctx, srv); err == nil {
		t.Fatal("执行失败应报错")
	}
	f.err = nil

	// Pod 列表：解析 JSON 并汇总状态
	f.output = `{"items":[{"metadata":{"name":"p1","namespace":"shuili","creationTimestamp":"2026-11-12T00:00:00Z"},"spec":{"nodeName":"n1","containers":[{"name":"c1","image":"app:v1"}]},"status":{"phase":"Running","podIP":"1.2.3.4","containerStatuses":[{"name":"c1","ready":true,"restartCount":2,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}]}`
	pods, err := c.ListPods(ctx, srv, "shuili")
	if err != nil || len(pods) != 1 {
		t.Fatalf("解析 Pod 失败: %v", err)
	}
	p := pods[0]
	if p.Status != "CrashLoopBackOff" || p.Ready != "1/1" || p.Restarts != 2 || p.Images[0] != "app:v1" || p.IP != "1.2.3.4" {
		t.Fatalf("Pod 信息不正确: %+v", p)
	}
	f.output = "not-json"
	if _, err := c.ListPods(ctx, srv, "shuili"); err == nil {
		t.Fatal("非法 JSON 应报错")
	}

	// 日志命令
	f.output = "log line"
	if logs, err := c.GetPodLogs(ctx, srv, "shuili", "p1", "c1", 99999); err != nil || logs != "log line" {
		t.Fatalf("日志 = %q %v", logs, err)
	}
	if cmd := f.cmds[len(f.cmds)-1]; !strings.Contains(cmd, "maxTailLines") && !strings.Contains(cmd, "--tail=4000") || !strings.Contains(cmd, "-c 'c1'") {
		t.Fatalf("日志命令不正确: %s", cmd)
	}

	// rollout / restart / set image
	f.output = ""
	if err := c.RolloutStatus(ctx, io.Discard, srv, "shuili", "demo", 0); err != nil {
		t.Fatal(err)
	}
	if err := c.RestartDeployment(ctx, srv, "shuili", "demo"); err != nil {
		t.Fatal(err)
	}
	f.err = errors.New("fail")
	if err := c.RestartDeployment(ctx, srv, "shuili", "demo"); err == nil {
		t.Fatal("重启失败应报错")
	}
	if err := c.SetImage(ctx, io.Discard, srv, "shuili", "demo", "demo", "app/demo:v1"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetImage(ctx, io.Discard, srv, "shuili", "demo", "demo", "bad,image"); err == nil {
		t.Fatal("含逗号镜像应被拒绝")
	}
	if err := c.SetImage(ctx, io.Discard, srv, "shuili", "BAD", "demo", "x:v1"); err == nil {
		t.Fatal("非法 Deployment 应被拒绝")
	}
}

func TestK8sFormatDuration(t *testing.T) {
	c := []struct {
		sec  int
		want string
	}{{5, "5s"}, {90, "1m"}, {3700, "1h"}, {90000, "1d"}}
	for _, x := range c {
		if got := k8s.FormatDurationForTest(durSec(x.sec)); got != x.want {
			t.Fatalf("%ds -> %s, want %s", x.sec, got, x.want)
		}
	}
}

func durSec(sec int) time.Duration { return time.Duration(sec) * time.Second }

func (f *fakeR) ReadRemoteFile(*sshpool.ServerConfig, string) (string, error) { return "", nil }
func (f *fakeR) WriteRemoteFile(*sshpool.ServerConfig, string, string) error  { return nil }
func (f *fakeR) UploadFile(context.Context, *sshpool.ServerConfig, string, string, func(int64, int64)) error {
	return nil
}
