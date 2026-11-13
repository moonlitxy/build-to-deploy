package tests

import (
	"strings"
	"testing"

	"build-to-deploy/internal/dockerops"
)

func TestDockerOpsParseList(t *testing.T) {
	out := strings.Join([]string{
		"app/demo:v1|sha256:aaa|512MB|2026-11-01 10:00:00 +0800 CST",
		"app/demo:v2|sha256:bbb|128MB|2026-11-02 10:00:00 +0800 CST",
		// 其它仓库、dangling、非法 tag、重复条目都应被过滤
		"other/app:v9|sha256:ccc|1MB|2026-11-03 10:00:00 +0800 CST",
		"app/demo:<none>|sha256:ddd|<none>|<none>",
		"app/demo:bad tag|sha256:eee|1MB|x",
		"app/demo:v1|sha256:aaa|512MB|2026-11-01 10:00:00 +0800 CST",
		"garbage-line",
		"",
	}, "\n")
	imgs := dockerops.ParseList(out, "app/demo")
	if len(imgs) != 2 {
		t.Fatalf("应解析出 2 个镜像，实际 %d: %+v", len(imgs), imgs)
	}
	if imgs[0].Tag != "v1" || imgs[0].ID != "sha256:aaa" || imgs[0].Size != "512MB" {
		t.Fatalf("字段解析错误: %+v", imgs[0])
	}
	if imgs[1].CreatedAt == "" {
		t.Fatalf("创建时间应保留: %+v", imgs[1])
	}
	if got := dockerops.ParseList("", "app/demo"); len(got) != 0 {
		t.Fatalf("空输出应为空列表: %+v", got)
	}
}

func TestDockerOpsUsedTags(t *testing.T) {
	out := "app/demo:v2\nother/app:v1\napp/demo:bad tag\n\n"
	used := dockerops.UsedTagsForTest(out, "app/demo")
	if !used["v2"] || len(used) != 1 {
		t.Fatalf("使用中的 tag 判定错误: %+v", used)
	}
}

func TestDockerOpsListCmdWhiteList(t *testing.T) {
	if _, err := dockerops.ListCmd("app/demo"); err != nil {
		t.Fatalf("合法镜像名不应报错: %v", err)
	}
	for _, bad := range []string{"", "app/demo;rm -rf /", "app/demo $(id)", "app/demo\nx"} {
		if _, err := dockerops.ListCmd(bad); err == nil {
			t.Fatalf("非法镜像名 %q 应被拒绝", bad)
		}
	}
}

func TestDockerOpsRemoveCmd(t *testing.T) {
	cmd, err := dockerops.RemoveCmd("app/demo", []string{"app/demo:v1", "app/demo:v2", "app/demo:v1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cmd, "docker image rm ") || strings.Count(cmd, "app/demo:v1") != 1 {
		t.Fatalf("删除命令应去重且以 docker image rm 开头: %s", cmd)
	}
	if !strings.Contains(cmd, "'app/demo:v2'") {
		t.Fatalf("引用应加引号: %s", cmd)
	}

	// 不属于本仓库、注入字符、空列表一律拒绝
	for _, refs := range [][]string{
		{},
		{"other/app:v1"},
		{"app/demo:v1; reboot"},
		{"app/demo:"},
	} {
		if _, err := dockerops.RemoveCmd("app/demo", refs); err == nil {
			t.Fatalf("应拒绝: %v", refs)
		}
	}
	if _, err := dockerops.RemoveCmd("bad name", []string{"bad name:v1"}); err == nil {
		t.Fatal("非法镜像名应被拒绝")
	}
}

func TestDockerOpsNilRunner(t *testing.T) {
	if _, err := dockerops.List(nil, nil, nil, "app/demo"); err == nil {
		t.Fatal("空执行器应报错")
	}
	if _, err := dockerops.Remove(nil, nil, nil, "app/demo", []string{"app/demo:v1"}); err == nil {
		t.Fatal("空执行器应报错")
	}
}
