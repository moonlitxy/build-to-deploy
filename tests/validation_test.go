// 本目录集中存放安全校验与核心行为的单测：被测对象均为已导出的纯函数，
// 避免把 _test.go 散落在各业务包中。
package tests

import (
	"strings"
	"testing"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/k8s"
	"build-to-deploy/internal/sshpool"
	"build-to-deploy/internal/upload"
)

// TestShellQuote 远程命令拼接的转义必须包裹单引号并转义内部单引号
func TestShellQuote(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"空字符串", "", "''"},
		{"普通镜像引用", "harbor.example.com/app:v1", "'harbor.example.com/app:v1'"},
		{"单引号转义", "a'b", "'a'\\''b'"},
		{"分号仅包裹", "a;b", "'a;b'"},
		{"换行仅包裹", "a\nb", "'a\nb'"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := sshpool.ShellQuote(c.in); got != c.want {
				t.Fatalf("ShellQuote(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestValidateFileName 文件名白名单：会被拼入远程 cp 命令，中文与特殊字符必须拒绝
func TestValidateFileName(t *testing.T) {
	valid := []string{"app.jar", "a", "A-1_2.3", strings.Repeat("a", 128)}
	for _, n := range valid {
		if err := upload.ValidateFileName(n); err != nil {
			t.Errorf("ValidateFileName(%q) 不应报错: %v", n, err)
		}
	}
	invalid := []string{"", "中文.jar", "a b", "a;b", "$a", ".a", "a/b", "a'b", strings.Repeat("a", 129)}
	for _, n := range invalid {
		if err := upload.ValidateFileName(n); err == nil {
			t.Errorf("ValidateFileName(%q) 应该报错", n)
		}
	}
}

// TestCheckName K8s 资源名仅允许小写 RFC 1123 label
func TestCheckName(t *testing.T) {
	valid := []string{"prod", "a", "order-service-1"}
	for _, n := range valid {
		if err := k8s.CheckName("命名空间", n); err != nil {
			t.Errorf("CheckName(%q) 不应报错: %v", n, err)
		}
	}
	invalid := []string{"", "Prod", "-a", "a_b", strings.Repeat("a", 64)}
	for _, n := range invalid {
		if err := k8s.CheckName("命名空间", n); err == nil {
			t.Errorf("CheckName(%q) 应该报错", n)
		}
	}
}

// validApp 返回一份合法的应用配置，各用例按需篡改单个字段
func validApp() *config.AppConfig {
	return &config.AppConfig{
		ID:          "order-service",
		Name:        "order-service",
		ServerID:    "s1",
		Type:        "jar",
		ArtifactDir: "/data/artifacts",
		BuildDir:    "/data/build",
		K8sYamlDir:  "/data/k8s",
		K8sYamlFile: "order-service.yaml",
		Image:       "harbor.example.com/app/order-service",
		Namespace:   "prod",
		Deployment:  "order-service",
		Container:   "order-service",
	}
}

// TestValidateApp 远程路径校验：根目录与 .. 必须拒绝，防止清理或构建命令越界
func TestValidateApp(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(a *config.AppConfig)
		wantErr string
	}{
		{"合法配置", func(a *config.AppConfig) {}, ""},
		{"产物目录为根", func(a *config.AppConfig) { a.ArtifactDir = "/" }, "根目录"},
		{"构建目录含双点", func(a *config.AppConfig) { a.BuildDir = "/data/../etc" }, ".."},
		{"大写命名空间", func(a *config.AppConfig) { a.Namespace = "Prod" }, "命名空间"},
		{"镜像名含空格", func(a *config.AppConfig) { a.Image = "harbor.example.com/a b" }, "镜像名"},
		{"镜像名含分号", func(a *config.AppConfig) { a.Image = "a;rm" }, "镜像名"},
		{"路径含空格", func(a *config.AppConfig) { a.ArtifactDir = "/data/a b" }, "绝对路径"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := validApp()
			c.mutate(a)
			err := config.ValidateApp(a)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("不应报错: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("报错应包含 %q, 实际: %v", c.wantErr, err)
			}
		})
	}
}

// TestFindServerStrict 找不到必须返回 nil，禁止兜底返回第一台（会连错机器）
func TestFindServerStrict(t *testing.T) {
	c := &config.Config{
		SSHServers: []*sshpool.ServerConfig{{ID: "s1", Host: "h", User: "u"}},
	}
	if got := c.FindServer("s1"); got == nil || got.ID != "s1" {
		t.Fatalf("存在的服务器应能找到, 实际: %+v", got)
	}
	for _, id := range []string{"nope", ""} {
		if got := c.FindServer(id); got != nil {
			t.Errorf("FindServer(%q) 应返回 nil, 实际: %+v", id, got)
		}
	}
}

// TestUpdateLocalArtifactPathUnknownApp 未知应用必须报错，禁止静默返回 nil
func TestUpdateLocalArtifactPathUnknownApp(t *testing.T) {
	c := &config.Config{Apps: []*config.AppConfig{validApp()}}
	// 错误路径在持久化之前返回，不会产生磁盘写入副作用
	if err := c.UpdateLocalArtifactPath("nope", "/tmp/x.jar"); err == nil {
		t.Fatal("未知应用应该报错")
	}
}
