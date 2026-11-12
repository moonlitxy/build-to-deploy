package tests

import (
	"build-to-deploy/internal/config"
	"os"
	"path/filepath"
	"testing"

	"build-to-deploy/internal/sshpool"
)

func writeCfg(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const sampleCfg = `
server:
  listen: "127.0.0.1:8080"
sshServers:
  - id: "s1"
    name: "主机"
    host: "10.0.0.1"
    port: 22
    user: "root"
apps:
  - id: "a1"
    name: "a1"
    type: "jar"
    serverId: "s1"
    image: "app/a1"
    artifactDir: "/data/a1"
    buildDir: "/data/a1-build"
    k8sYamlDir: "/data/yaml"
    k8sYamlFile: "a1.yaml"
`

func TestConfigLoadNormalizeDefaults(t *testing.T) {
	cfg, err := config.Load(writeCfg(t, sampleCfg))
	if err != nil {
		t.Fatal(err)
	}
	app := cfg.FindApp("a1")
	if app.DeployMode != config.DeployModeK8s {
		t.Fatalf("默认部署形态应为 k8s: %q", app.DeployMode)
	}
	if app.Namespace != "default" || app.Deployment != "a1" || app.Container != "a1" {
		t.Fatalf("默认命名空间/Deployment/容器不正确: %+v", app)
	}
	if app.RolloutTimeout != 300 || app.DeployCmd != "kubectl apply -f {{.K8sYamlPath}}" {
		t.Fatalf("默认值不正确: %+v", app)
	}

	// 不存在的配置文件会自动生成默认配置
	p := filepath.Join(t.TempDir(), "new.yaml")
	cfg, err = config.Load(p)
	if err != nil || len(cfg.Apps) == 0 {
		t.Fatalf("应生成默认配置: %v", err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatal("默认配置应落盘")
	}

	// 非法 YAML
	if _, err := config.Load(writeCfg(t, "server: [unclosed")); err == nil {
		t.Fatal("非法 YAML 应报错")
	}
}

func TestConfigNormalizeDockerMode(t *testing.T) {
	cfg, err := config.Load(writeCfg(t, sampleCfg+`
    deployMode: "docker"
`))
	if err != nil {
		t.Fatal(err)
	}
	app := cfg.FindApp("a1")
	if app.DockerContainer != "a1" {
		t.Fatalf("容器名默认应取 Deployment: %q", app.DockerContainer)
	}
	if app.DeployCmd != "docker run -d --name {{.Container}} {{.Image}}:{{.Tag}}" {
		t.Fatalf("Docker 形态默认部署命令不正确: %q", app.DeployCmd)
	}
}

func TestConfigValidateApp(t *testing.T) {
	good := config.AppConfig{ID: "a1", Name: "a1", Image: "app/a1", ArtifactDir: "/data/a", BuildDir: "/data/b", K8sYamlDir: "/data/y", K8sYamlFile: "a.yaml", Namespace: "prod", Deployment: "d", Container: "c"}
	if err := config.ValidateApp(&good); err != nil {
		t.Fatalf("合法配置: %v", err)
	}
	bad := []config.AppConfig{
		{ID: "-bad"},
		{ID: "ok", Namespace: "BAD NS"},
		{ID: "ok", Deployment: "UPPER"},
		{ID: "ok", Container: "bad name"},
		{ID: "ok", Image: "app/a;rm -rf"},
		{ID: "ok", ArtifactDir: "/"},
		{ID: "ok", BuildDir: "/data/../etc"},
		{ID: "ok", K8sYamlDir: "/data/a b"},
		{ID: "ok", K8sYamlFile: "a.txt"},
		{ID: "ok", DeployMode: "nomad"},
	}
	for i := range bad {
		if err := config.ValidateApp(&bad[i]); err == nil {
			t.Fatalf("用例 %d 应校验失败: %+v", i, bad[i])
		}
	}
}

func TestConfigValidateDocker(t *testing.T) {
	good := config.AppConfig{DeployMode: config.DeployModeDocker, DockerContainer: "app-1", DockerPort: "8080:80", DockerVolumes: "/a:/b,/c:/d", DockerEnv: "A=B,C=D", DockerRunOpts: "--restart=always", DockerHealthPath: "/health"}
	if err := config.ValidateDocker(&good); err != nil {
		t.Fatalf("合法 Docker 配置: %v", err)
	}
	if !good.DockerConfigured() {
		t.Fatal("DockerConfigured 应返回 true")
	}
	var empty config.AppConfig
	if empty.DockerConfigured() {
		t.Fatal("未配置容器名时 DockerConfigured 应为 false")
	}
	for _, bad := range []config.AppConfig{
		{DockerContainer: ""},
		{DockerContainer: "name with space"},
		{DockerContainer: "ok", DockerPort: "80:80:80"},
		{DockerContainer: "ok", DockerVolumes: "/a:/b;id"},
		{DockerContainer: "ok", DockerEnv: "A=$(id)"},
		{DockerContainer: "ok", DockerRunOpts: "--privileged&&rm"},
		{DockerContainer: "ok", DockerHealthPath: "relative"},
	} {
		if err := config.ValidateDocker(&bad); err == nil {
			t.Fatalf("应校验失败: %+v", bad)
		}
	}
}

func homeDir() string {
	h, _ := os.UserHomeDir()
	return h
}

func TestConfigConfigMutationAndBackups(t *testing.T) {
	p := writeCfg(t, sampleCfg)
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.UpdateServer(&sshpool.ServerConfig{ID: "s2", Name: "第二台", Host: "10.0.0.2", Port: 22, User: "root"}); err != nil {
		t.Fatal(err)
	}
	if cfg.FindServer("s2") == nil || cfg.FindServer("nope") != nil {
		t.Fatal("FindServer 行为不正确")
	}
	if len(cfg.ListServers()) != 2 {
		t.Fatalf("服务器数量 = %d", len(cfg.ListServers()))
	}

	app := *cfg.FindApp("a1")
	app.Description = "改过的描述"
	if err := cfg.UpdateApp(&app); err != nil {
		t.Fatal(err)
	}
	if cfg.FindApp("a1").Description != "改过的描述" {
		t.Fatal("应用更新未生效")
	}
	if err := cfg.UpdateLocalArtifactPath("a1", "~/x.jar"); err != nil {
		t.Fatal(err)
	}
	if got := cfg.FindApp("a1").ResolveLocalArtifactPath(); got != filepath.Join(homeDir(), "x.jar") {
		t.Fatalf("本地产物路径应在解析时展开 ~: %q", got)
	}
	if err := cfg.UpdateApp(&config.AppConfig{ID: "bad id"}); err == nil {
		t.Fatal("非法应用应被拒绝")
	}

	backups, err := cfg.Backups()
	if err != nil || len(backups) == 0 {
		t.Fatalf("应有配置备份: %v", err)
	}
	if err := cfg.Restore(backups[0].Name); err != nil {
		t.Fatalf("恢复备份: %v", err)
	}
	if err := cfg.Restore("../evil"); err == nil {
		t.Fatal("非法备份名应被拒绝")
	}
	if err := cfg.Restore("missing.yaml"); err == nil {
		t.Fatal("不存在的备份应报错")
	}

	if err := cfg.DeleteApp("a1"); err != nil {
		t.Fatal(err)
	}
	if cfg.FindApp("a1") != nil {
		t.Fatal("应用应被删除")
	}
	if err := cfg.DeleteServer("s2"); err != nil {
		t.Fatal(err)
	}
	if cfg.FindServer("s2") != nil {
		t.Fatal("服务器应被删除")
	}
	if cfg.ServerSettingsSnapshot().Listen == "" {
		t.Fatal("服务设置快照不应为空")
	}
}

func TestConfigWithOverridesAndExpandHome(t *testing.T) {
	app := &config.AppConfig{BuildDir: "/data/old", BuildCmd: "old"}
	cp := app.WithOverrides("/data/new/", "new-cmd")
	if cp.BuildDir != "/data/new" || cp.BuildCmd != "new-cmd" {
		t.Fatalf("覆盖未生效: %+v", cp)
	}
	if app.BuildDir != "/data/old" {
		t.Fatal("不应修改原配置")
	}
	if got := app.WithOverrides("", "").BuildCmd; got != "old" {
		t.Fatal("空值不应覆盖")
	}
	home := homeDir()
	if config.ExpandHome("~/a") != filepath.Join(home, "a") || config.ExpandHome("~") != home || config.ExpandHome("/abs") != "/abs" {
		t.Fatal("ExpandHome 不正确")
	}
}
