package tests

import (
	"strings"
	"testing"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/dockerrun"
)

func base() *config.AppConfig {
	return &config.AppConfig{
		DockerContainer:  "demo",
		DockerPort:       "18081:8080",
		DockerVolumes:    "/data/logs:/app/logs, /data/up:/app/up",
		DockerEnv:        "TZ=Asia/Shanghai,A=B",
		DockerRunOpts:    "--restart=always",
		DockerHealthPath: "/actuator/health",
	}
}

func TestDockerRunRunArgs(t *testing.T) {
	args, err := dockerrun.RunArgs(base())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-d", "--name", "demo", "-p", "18081:8080", "-v", "/data/logs:/app/logs", "-v", "/data/up:/app/up", "-e", "TZ=Asia/Shanghai", "-e", "A=B", "--restart=always"}
	if strings.Join(args, " ") != strings.Join(want, " ") {
		t.Fatalf("args = %q", args)
	}

	// 最小配置：只保留必需参数
	args, err = dockerrun.RunArgs(&config.AppConfig{DockerContainer: "x"})
	if err != nil || strings.Join(args, " ") != "-d --name x" {
		t.Fatalf("最小配置 args = %q, %v", args, err)
	}

	// 非法容器名被拒绝
	if _, err := dockerrun.RunArgs(&config.AppConfig{DockerContainer: "bad name"}); err == nil {
		t.Fatal("非法容器名应报错")
	}
	if _, err := dockerrun.RunArgs(&config.AppConfig{}); err == nil {
		t.Fatal("缺少容器名应报错")
	}
}

func TestDockerRunRunCmd(t *testing.T) {
	cmd, err := dockerrun.RunCmd(base(), "app/demo:v1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"docker rm -f 'demo'", "docker run '-d' '--name' 'demo'", "'app/demo:v1.0.0'"} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("命令缺少 %q:\n%s", want, cmd)
		}
	}
	for _, bad := range []string{"", "app/demo;rm", "app/demo v1", "x$(id)"} {
		if _, err := dockerrun.RunCmd(base(), bad); err == nil {
			t.Fatalf("非法镜像 %q 应报错", bad)
		}
	}
	if _, err := dockerrun.RunCmd(&config.AppConfig{DockerContainer: "bad name"}, "x:v1"); err == nil {
		t.Fatal("非法配置应报错")
	}
}

func TestDockerRunInspectAndParse(t *testing.T) {
	cmd, err := dockerrun.InspectCmdFor(&config.AppConfig{DockerContainer: "demo"})
	if err != nil || !strings.Contains(cmd, "'demo'") {
		t.Fatalf("inspect 命令 = %q, %v", cmd, err)
	}
	if _, err := dockerrun.InspectCmdFor(&config.AppConfig{}); err == nil {
		t.Fatal("缺少容器名应报错")
	}
	st, img := dockerrun.ParseInspect("  running app/demo:v1  \n")
	if st != "running" || img != "app/demo:v1" {
		t.Fatalf("解析 = %q %q", st, img)
	}
	if st, img := dockerrun.ParseInspect(""); st != "" || img != "" {
		t.Fatal("空输出应返回空")
	}
	if st, _ := dockerrun.ParseInspect("exited"); st != "exited" {
		t.Fatal("只有状态时应能解析")
	}
}

func TestDockerRunSameImage(t *testing.T) {
	if !dockerrun.SameImage("sha256:abc", "app:v1", "sha256:abc") {
		t.Fatal("镜像 ID 相同应判定一致")
	}
	if !dockerrun.SameImage("app:v1", "app:v1", "") {
		t.Fatal("旧版 docker 记录 Tag 时应按字符串比较")
	}
	if dockerrun.SameImage("app:v2", "app:v1", "id2") {
		t.Fatal("不同镜像不应判定一致")
	}
	if dockerrun.SameImage("", "app:v1", "") {
		t.Fatal("空值不应判定一致")
	}
}

func TestDockerRunHealthAndReadyCmd(t *testing.T) {
	if dockerrun.HealthCmd(&config.AppConfig{}) != "" {
		t.Fatal("未配置健康路径时不应生成探测命令")
	}
	h := dockerrun.HealthCmd(base())
	if !strings.Contains(h, "http://127.0.0.1:8080/actuator/health") {
		t.Fatalf("健康探测应使用容器端口: %s", h)
	}
	if h := dockerrun.HealthCmd(&config.AppConfig{DockerHealthPath: "/ping"}); !strings.Contains(h, "http://127.0.0.1/ping") {
		t.Fatalf("无端口映射时不应带端口: %s", h)
	}

	cmd, err := dockerrun.ReadyCmd(base(), 30)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cmd, "docker exec 'demo'") || !strings.Contains(cmd, "READY") || !strings.Contains(cmd, "TIMEOUT") {
		t.Fatalf("就绪等待命令不正确:\n%s", cmd)
	}
	// 无健康检查：只等 running
	cmd, err = dockerrun.ReadyCmd(&config.AppConfig{DockerContainer: "demo"}, 0)
	if err != nil || strings.Contains(cmd, "docker exec") || !strings.Contains(cmd, "running*") {
		t.Fatalf("无健康检查的命令不正确:\n%s %v", cmd, err)
	}
	if _, err := dockerrun.ReadyCmd(&config.AppConfig{DockerContainer: "bad name"}, 30); err == nil {
		t.Fatal("非法配置应报错")
	}
}

func TestDockerRunQuote(t *testing.T) {
	if dockerrun.Quote("a'b") != `'a'\''b'` {
		t.Fatalf("Quote = %s", dockerrun.Quote("a'b"))
	}
}
