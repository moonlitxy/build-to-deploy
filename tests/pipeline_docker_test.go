package tests

import (
	"build-to-deploy/internal/pipeline"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/k8s"
	"build-to-deploy/internal/sshpool"
	"build-to-deploy/internal/store"
	"build-to-deploy/internal/upload"
)

// fakeSSH 记录并模拟所有远程命令，用于在不连真机的情况下验证流水线行为
type fakeSSH struct {
	mu       sync.Mutex
	runs     []string
	outputs  map[string]string // 命令子串 -> 返回值（空命令说明用 ExecuteFunc）
	files    map[string]string
	runErr   error // RunCommand 统一返回的错误
	execErr  error
	uploaded []string
}

func newFakeSSH() *fakeSSH {
	return &fakeSSH{outputs: map[string]string{}, files: map[string]string{}}
}

func (f *fakeSSH) RunCommand(ctx context.Context, srv *sshpool.ServerConfig, cmd string, w io.Writer) error {
	f.mu.Lock()
	f.runs = append(f.runs, cmd)
	err := f.runErr
	f.mu.Unlock()
	fmt.Fprintf(w, "$ %s\n", cmd)
	return err
}

func (f *fakeSSH) ExecuteOutput(ctx context.Context, srv *sshpool.ServerConfig, cmd string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range f.outputs {
		if strings.Contains(cmd, k) {
			return v, nil
		}
	}
	return "", f.execErr
}

func (f *fakeSSH) ReadRemoteFile(srv *sshpool.ServerConfig, path string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.files[path]; ok {
		return c, nil
	}
	return "", errors.New("文件不存在")
}

func (f *fakeSSH) WriteRemoteFile(srv *sshpool.ServerConfig, path, content string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[path] = content
	return nil
}

func (f *fakeSSH) UploadFile(ctx context.Context, srv *sshpool.ServerConfig, localPath, remotePath string, fn func(int64, int64)) error {
	f.mu.Lock()
	f.uploaded = append(f.uploaded, remotePath)
	f.mu.Unlock()
	return nil
}

func (f *fakeSSH) MarkSimulated(srv *sshpool.ServerConfig) {}

func (f *fakeSSH) commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.runs...)
}

// newDockerTestEnv 构造一个指向假 SSH 的引擎与容器形态应用（全部走真实命令分支）
func newDockerTestEnv(t *testing.T) (*pipeline.Engine, *config.Config, *store.Store, *fakeSSH, *config.AppConfig) {
	t.Helper()
	dir := t.TempDir()
	// 产物所在目录由 localArtifactPath 直接指定，避免测试之间因为"同一份产物"互相影响
	artifacts := filepath.Join(dir, "artifacts")
	proj := filepath.Join(dir, "proj")
	for _, d := range []string{artifacts, proj} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(artifacts, "app.jar"), []byte("PK\x03\x04jar"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proj, "Dockerfile"), []byte("FROM x\nCOPY app.jar /app.jar\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfgPath := filepath.Join(dir, "config.yaml")
	cfgYAML := fmt.Sprintf(`
server:
  listen: "127.0.0.1:0"
  dataDir: %q
sshServers:
  - id: "s1"
    name: "测试机"
    host: "127.0.0.1"
    port: 22
    user: "root"
apps:
  - id: "demo"
    name: "demo"
    type: "jar"
    serverId: "s1"
    image: "shuili/demo"
    localArtifactPath: %q
    artifactDir: %q
    buildDir: %q
    buildCmd: "cd {{.BuildDir}} && docker build -t {{.Image}}:{{.Tag}} ."
    deployMode: "docker"
    dockerContainer: "demo"
    dockerPort: "18081:8080"
    dockerVolumes: "/data/demo/logs:/app/logs"
    dockerEnv: "TZ=Asia/Shanghai"
    dockerRunOpts: "--restart=always"
    rolloutTimeout: 30
    autoRollback: true
`, filepath.Join(dir, "data"), filepath.Join(artifacts, "app.jar"), filepath.Join(dir, "artifacts"), proj)
	if err := os.WriteFile(cfgPath, []byte(cfgYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dir, "releases.json"))
	if err != nil {
		t.Fatal(err)
	}
	ups, err := upload.NewManager(filepath.Join(dir, "uploads"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeSSH()
	eng := pipeline.NewEngine(cfg, st, ups, nil, k8s.NewClient(nil), filepath.Join(dir, "data"))
	eng.SSH = fake
	return eng, cfg, st, fake, cfg.Apps[0]
}

// waitRelease 等待发布记录进入终态
func waitRelease(t *testing.T, st *store.Store, id string, timeout time.Duration) *store.Release {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rel, err := st.Get(id)
		if err == nil && rel.Done() {
			return rel
		}
		time.Sleep(20 * time.Millisecond)
	}
	rel, _ := st.Get(id)
	t.Fatalf("发布 %s 未在 %s 内结束: status=%v", id, timeout, rel)
	return nil
}

func TestPDockerDockerModeDeploy(t *testing.T) {
	eng, _, st, fake, app := newDockerTestEnv(t)
	rel, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatalf("StartDeploy: %v", err)
	}
	if rel.Mode != config.DeployModeDocker {
		t.Fatalf("记录的模式 = %q", rel.Mode)
	}
	done := waitRelease(t, st, rel.ID, 10*time.Second)
	if done.Status != store.StatusSuccess {
		t.Fatalf("状态 = %s, err=%s\n日志: %s", done.Status, done.Error, readLog(t, eng, rel.ID))
	}
	if done.ImageFP == "" {
		t.Fatal("发布记录应写入构建输入指纹")
	}
	log := readLog(t, eng, rel.ID)
	for _, want := range []string{"部署形态: Docker 容器 (demo)", "容器 demo 已就绪"} {
		if !strings.Contains(log, want) {
			t.Fatalf("日志缺少 %q:\n%s", want, log)
		}
	}
	if strings.Contains(log, "kubectl") {
		t.Fatalf("容器形态不应执行 kubectl:\n%s", log)
	}
	for _, cmd := range fake.commands() {
		if strings.Contains(cmd, "kubectl") || strings.Contains(cmd, "sftp") {
			t.Fatalf("容器形态不应出现 K8s/SFTP 命令: %s", cmd)
		}
	}
}

func TestPDockerDockerModeSameArtifactWarned(t *testing.T) {
	eng, _, st, _, app := newDockerTestEnv(t)
	first, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := waitRelease(t, st, first.ID, 10*time.Second); got.Status != store.StatusSuccess {
		t.Fatalf("首次发布失败: %s", got.Error)
	}

	// 同产物换新 Tag：不拦截（用户可能确实要保留同内容的另一个 Tag），但发布日志必须告警
	second, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.1", UseLocalArtifact: true})
	if err != nil {
		t.Fatalf("同产物换 Tag 应允许发布（只告警）: %v", err)
	}
	if got := waitRelease(t, st, second.ID, 10*time.Second); got.Status != store.StatusSuccess {
		t.Fatalf("第二次发布失败: %s", got.Error)
	}
	if got := readLog(t, eng, second.ID); !strings.Contains(got, "完全相同") {
		t.Fatalf("发布日志应提示内容相同:\n%s", got)
	}
}

func TestPDockerDockerModeChangedArtifactAllowed(t *testing.T) {
	eng, _, st, _, app := newDockerTestEnv(t)
	first, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	waitRelease(t, st, first.ID, 10*time.Second)

	// 产物与构建上下文都变化后，指纹必然变化
	if err := os.WriteFile(app.ResolveLocalArtifactPath(), []byte("PK\x03\x04jar-v2"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(app.BuildDir, "Dockerfile"), []byte("FROM x\nCOPY app.jar /app.jar\nLABEL v=2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rel, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.1", UseLocalArtifact: true})
	if err != nil {
		t.Fatalf("产物变化后应允许发布: %v", err)
	}
	waitRelease(t, st, rel.ID, 10*time.Second)
}

func TestPDockerDockerModeValidateRejectsBadConfig(t *testing.T) {
	_, _, _, _, app := newDockerTestEnv(t)
	cases := []config.AppConfig{
		{DeployMode: "docker", DockerContainer: "bad name"},
		{DeployMode: "docker", DockerContainer: "ok", DockerPort: "80:abc"},
		{DeployMode: "docker", DockerContainer: "ok", DockerVolumes: "/a:/b;rm -rf /"},
		{DeployMode: "docker", DockerContainer: "ok", DockerEnv: "$(id)"},
		{DeployMode: "docker", DockerContainer: "ok", DockerRunOpts: "--name x && rm"},
		{DeployMode: "docker", DockerContainer: "ok", DockerHealthPath: "health"},
		{DeployMode: "swarm", DockerContainer: "ok"},
	}
	base := *app
	for i := range cases {
		c := cases[i]
		c.Image = base.Image
		c.ArtifactDir = base.ArtifactDir
		c.BuildDir = base.BuildDir
		c.K8sYamlDir = ""
		if err := config.ValidateApp(&c); err == nil {
			t.Fatalf("用例 %d 应校验失败: %+v", i, c)
		}
	}
	good := base
	good.DeployMode = config.DeployModeDocker
	good.DockerPort, good.DockerVolumes, good.DockerEnv = "18081:8080", "/a:/b,/c:/d", "A=B"
	good.DockerRunOpts, good.DockerHealthPath = "--restart=always", "/actuator/health"
	if err := config.ValidateApp(&good); err != nil {
		t.Fatalf("合法配置不应报错: %v", err)
	}
}

func TestPDockerDockerModeAutoRollback(t *testing.T) {
	eng, _, st, _, app := newDockerTestEnv(t)
	first, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	waitRelease(t, st, first.ID, 10*time.Second)
	if err := os.WriteFile(app.ResolveLocalArtifactPath(), []byte("PK\x03\x04jar-v2"), 0o644); err != nil {
		t.Fatal(err)
	}

	r := pipeline.NewRunnerLike(eng, context.Background(), &store.Release{ID: "rollback-test", Tag: "v1.0.1", Image: "shuili/demo:v1.0.1", PrevImage: "shuili/demo:v1.0.0"}, app, eng.Cfg.SSHServers[0], pipeline.NewHub(filepath.Join(t.TempDir(), "run.log")), "", nil)
	defer r.CloseHub()
	logFile := filepath.Join(t.TempDir(), "rollback.log")
	r.SetHub(pipeline.NewHub(logFile))
	defer r.CloseHub()
	pipeline.TryAutoRollbackForTest(r, errors.New("容器未就绪"))
	b, _ := os.ReadFile(logFile)
	log := string(b)
	if !strings.Contains(log, "shuili/demo:v1.0.0") || !strings.Contains(log, "已自动回滚") && !strings.Contains(log, "自动回滚") {
		t.Fatalf("自动回滚应打印回滚命令与结果:\n%s", log)
	}
}

func TestPDockerDockerModeCanceledSkipsRollback(t *testing.T) {
	_, _, _, fake, app := newDockerTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := pipeline.NewRunnerLike(nil, ctx, &store.Release{Image: "x:v2", PrevImage: "x:v1"}, app, &sshpool.ServerConfig{Host: "127.0.0.1"}, pipeline.NewHub(filepath.Join(t.TempDir(), "run.log")), "", nil)
	defer r.CloseHub()
	pipeline.TryAutoRollbackForTest(r, context.Canceled)
	if len(fake.commands()) != 0 {
		t.Fatal("用户取消时不应自动回滚")
	}
}

func TestPDockerDockerModeRollbackRelease(t *testing.T) {
	eng, _, st, _, app := newDockerTestEnv(t)
	first, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	waitRelease(t, st, first.ID, 10*time.Second)
	if err := os.WriteFile(app.ResolveLocalArtifactPath(), []byte("PK\x03\x04jar-v2"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.1", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	waitRelease(t, st, second.ID, 10*time.Second)
	// 记录进入终态后，流水线 goroutine 还要收尾（释放并发锁）：短暂等待避免与"正在发布中"竞争
	deadline := time.Now().Add(3 * time.Second)
	for pipeline.IsRunning(eng, app.ID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	rb, err := eng.StartRollback(pipeline.RollbackRequest{ReleaseID: first.ID})
	if err != nil {
		t.Fatalf("StartRollback: %v", err)
	}
	done := waitRelease(t, st, rb.ID, 10*time.Second)
	if done.Status != store.StatusSuccess {
		t.Fatalf("回滚失败: %s\n%s", done.Error, readLog(t, eng, rb.ID))
	}
	if done.Mode != config.DeployModeDocker {
		t.Fatalf("回滚记录模式 = %q", done.Mode)
	}
	log := readLog(t, eng, rb.ID)
	if !strings.Contains(log, "demo:v1.0.0") {
		t.Fatalf("回滚应使用 v1.0.0 镜像:\n%s", log)
	}
}

func TestPDockerDockerModeConcurrentLock(t *testing.T) {
	eng, _, st, _, app := newDockerTestEnv(t)
	defer pipeline.MarkRunning(eng, app.ID)()
	if _, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true}); err == nil {
		t.Fatal("同一应用并发发布应被拒绝")
	}
	_ = st
}

// readLog 读取发布日志文件
func readLog(t *testing.T, eng *pipeline.Engine, id string) string {
	t.Helper()
	b, err := os.ReadFile(eng.LogPath(id))
	if err != nil {
		return ""
	}
	return string(b)
}
