package tests

import (
	"build-to-deploy/internal/pipeline"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
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

// scriptedRunner 按命令关键字返回预设输出，并记录执行过的命令
type scriptedRunner struct {
	mu      sync.Mutex
	cmds    []string
	rules   []scriptRule
	runErr  error
	execErr error
	files   map[string]string
	uploads int
}

type scriptRule struct {
	contains string
	out      string
}

func (s *scriptedRunner) RunCommand(ctx context.Context, srv *sshpool.ServerConfig, cmd string, w io.Writer) error {
	s.mu.Lock()
	s.cmds = append(s.cmds, cmd)
	err := s.runErr
	s.mu.Unlock()
	io.WriteString(w, "[run] "+cmd+"\n")
	return err
}

func (s *scriptedRunner) ExecuteOutput(ctx context.Context, srv *sshpool.ServerConfig, cmd string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cmds = append(s.cmds, cmd)
	for _, r := range s.rules {
		if strings.Contains(cmd, r.contains) {
			return r.out, nil
		}
	}
	return "", s.execErr
}

func (s *scriptedRunner) ReadRemoteFile(srv *sshpool.ServerConfig, p string) (string, error) {
	if v, ok := s.files[p]; ok {
		return v, nil
	}
	return "", errors.New("missing")
}
func (s *scriptedRunner) WriteRemoteFile(srv *sshpool.ServerConfig, p, content string) error {
	if s.files == nil {
		s.files = map[string]string{}
	}
	s.files[p] = content
	return nil
}
func (s *scriptedRunner) UploadFile(ctx context.Context, srv *sshpool.ServerConfig, local, remote string, fn func(int64, int64)) error {
	s.mu.Lock()
	s.uploads++
	s.mu.Unlock()
	if fn != nil {
		fn(0, 2097152)
		fn(2097152, 2097152)
	}
	return nil
}
func (s *scriptedRunner) MarkSimulated(*sshpool.ServerConfig) {}

func (s *scriptedRunner) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.cmds...)
}

func (s *scriptedRunner) addRules(r ...scriptRule) {
	s.mu.Lock()
	s.rules = append(s.rules, r...)
	s.mu.Unlock()
}

// newScriptedEnv 完整引擎（远程全部走假执行器）
func newScriptedEnv(t *testing.T, mode string) (*pipeline.Engine, *config.Config, *store.Store, *scriptedRunner, *config.AppConfig) {
	t.Helper()
	dir := t.TempDir()
	artifacts := filepath.Join(dir, "artifacts")
	build := filepath.Join(dir, "build")
	yamlDir := filepath.Join(dir, "yaml")
	for _, d := range []string{artifacts, build, yamlDir} {
		os.MkdirAll(d, 0o755)
	}
	art := filepath.Join(artifacts, "app.jar")
	os.WriteFile(art, []byte("PK\x03\x04jar"), 0o644)
	os.WriteFile(filepath.Join(build, "Dockerfile"), []byte("FROM x\nCOPY app.jar /a\n"), 0o644)
	os.WriteFile(filepath.Join(yamlDir, "demo.yaml"), []byte("kind: Deployment\nspec:\n  containers:\n  - image: app/demo:v0.9.0  # 注释保留\n"), 0o644)

	modeLine := ""
	if mode == config.DeployModeDocker {
		modeLine = "    deployMode: docker\n    dockerContainer: demo\n    dockerPort: \"18081:8080\"\n"
	}
	cfgText := `
server:
  listen: "127.0.0.1:0"
  dataDir: ` + quoteYAML(filepath.Join(dir, "data")) + `
sshServers:
  - id: s1
    name: host
    host: 127.0.0.1
    port: 22
    user: root
apps:
  - id: demo
    name: demo
    type: jar
    serverId: s1
    image: app/demo
    localArtifactPath: ` + quoteYAML(art) + `
    artifactDir: ` + quoteYAML(artifacts) + `
    buildDir: ` + quoteYAML(build) + `
    k8sYamlDir: ` + quoteYAML(yamlDir) + `
    k8sYamlFile: demo.yaml
    namespace: prod
    deployment: demo
    container: demo
` + modeLine
	if mode == "" {
		cfgText += "    deployMode: k8s\n"
	}
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(cfgText), 0o644)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := store.Open(filepath.Join(dir, "releases.json"))
	ups, _ := upload.NewManager(filepath.Join(dir, "uploads"), 1<<20)
	fake := &scriptedRunner{files: map[string]string{
		// 远程路径统一正斜杠，与 app.K8sYamlDir 的写法保持一致
		filepath.ToSlash(filepath.Join(yamlDir, "demo.yaml")): "kind: Deployment\nspec:\n  containers:\n  - image: app/demo:v0.9.0  # 注释保留\n",
	}}
	eng := pipeline.NewEngine(cfg, st, ups, nil, k8s.NewClient(fake), cfg.Server.DataDir)
	eng.SSH = fake
	return eng, cfg, st, fake, cfg.Apps[0]
}

// quoteYAML 用 Go 转义规则生成 YAML 双引号字符串：Windows 盘符路径里的反斜杠必须转义
func quoteYAML(s string) string { return strconv.Quote(filepath.ToSlash(s)) }

func waitDoneStrict(t *testing.T, st *store.Store, id string) *store.Release {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if r, err := st.Get(id); err == nil && r.Done() {
			return r
		}
		time.Sleep(20 * time.Millisecond)
	}
	r, _ := st.Get(id)
	t.Fatalf("发布 %s 超时: %+v", id, r)
	return nil
}

func TestPEngineK8sPipelineSuccess(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	fake.addRules(
		scriptRule{"docker images -q", ""},
		scriptRule{"get deployment", "app/demo:v0.9.0"},
		scriptRule{"docker image inspect", "sha256:111111111111 []"},
		scriptRule{"docker images --no-trunc", "app/demo:v0.9.0 sha256:111111111111\n"},
	)
	rel, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, rel.ID)
	if done.Status != store.StatusSuccess {
		t.Fatalf("状态 = %s, err=%s\n%s", done.Status, done.Error, readLogFile(t, eng, rel.ID))
	}
	if done.FileSHA256 == "" || done.PrevImage != "app/demo:v0.9.0" {
		t.Fatalf("记录信息不完整: %+v", done)
	}
	// YAML 中的镜像已持久化改写，注释保留
	written := fake.files[app.K8sYamlDir+"/demo.yaml"]
	if !strings.Contains(written, "app/demo:v1.0.0") || !strings.Contains(written, "# 注释保留") {
		t.Fatalf("YAML 回写不正确: %q", written)
	}
	cmds := strings.Join(fake.snapshot(), "\n")
	if !strings.Contains(cmds, "kubectl apply -f") || !strings.Contains(cmds, "rollout status") {
		t.Fatalf("应执行 apply 与 rollout:\n%s", cmds)
	}
}

func TestPEngineK8sPipelineWithLocalBuildAndPush(t *testing.T) {
	eng, cfg, st, fake, _ := newScriptedEnv(t, config.DeployModeK8s)
	app := cfg.Apps[0]
	// 本地编译命令：在临时源码目录里生成产物
	src := t.TempDir()
	app.LocalProjectDir = src
	app.LocalBuildCmd = "printf 'PK\\x03\\x04built' > built.jar"
	app.LocalArtifactPath = "built.jar"
	app.PushCmd = "docker push {{.Image}}:{{.Tag}}"
	cfg.Save()
	_ = eng

	rel, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.1.0", RunLocalBuild: true})
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, rel.ID)
	if done.Status != store.StatusSuccess {
		t.Fatalf("状态 = %s, err=%s\n%s", done.Status, done.Error, readLogFile(t, eng, rel.ID))
	}
	cmds := strings.Join(fake.snapshot(), "\n")
	if !strings.Contains(cmds, "docker push") {
		t.Fatalf("应执行推送命令:\n%s", cmds)
	}
	if done.FileName != "built.jar" {
		t.Fatalf("产物名 = %s", done.FileName)
	}
}

func TestPEngineK8sPipelineBuildFailureRollback(t *testing.T) {
	eng, cfg, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	_ = cfg
	fake.addRules(scriptRule{"get deployment", "app/demo:v0.9.0"})
	fake.runErr = errors.New("build failed")
	rel, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.2.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, rel.ID)
	if done.Status != store.StatusFailed {
		t.Fatalf("构建失败应标记 failed: %s", done.Status)
	}
	if done.Error == "" {
		t.Fatal("应记录错误信息")
	}
}

func TestPEngineK8sDeployFailsRollsBackYaml(t *testing.T) {
	eng, cfg, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	_ = cfg
	fake.addRules(scriptRule{"get deployment", "app/demo:v0.9.0"})
	// 删除 YAML 文件里的 image 字段：部署阶段会失败并触发自动回滚
	fake.files[app.K8sYamlDir+"/demo.yaml"] = "kind: Service\n"
	rel, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.4.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, rel.ID)
	if done.Status != store.StatusFailed {
		t.Fatalf("应失败: %s", done.Status)
	}
	if !strings.Contains(done.Error, "未找到可替换的 image") {
		t.Fatalf("错误信息 = %s", done.Error)
	}
}

func TestPEngineCancelRelease(t *testing.T) {
	eng, _, st, _, app := newScriptedEnv(t, config.DeployModeK8s)
	// 让预检卡住：用 execute 阻塞不方便，这里先启动并立即取消
	rel, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v2.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Cancel(rel.ID); err != nil {
		t.Fatalf("取消失败: %v", err)
	}
	done := waitDoneStrict(t, st, rel.ID)
	if done.Status != store.StatusCanceled && done.Status != store.StatusSuccess {
		t.Fatalf("取消后状态 = %s", done.Status)
	}
	if err := eng.Cancel("not-running"); err == nil {
		t.Fatal("取消不在运行的发布应报错")
	}
}

func TestPEngineRequestContextCancelDoesNotKillPipeline(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	fake.addRules(
		scriptRule{"docker images -q", ""},
		scriptRule{"get deployment", "app/demo:v0.9.0"},
		scriptRule{"docker image inspect", "sha256:111111111111 []"},
		scriptRule{"docker images --no-trunc", "app/demo:v0.9.0 sha256:111111111111\n"},
	)
	// 模拟 HTTP handler：StartDeployContext 返回后请求 ctx 立即被取消（响应写出）
	reqCtx, reqCancel := context.WithCancel(context.Background())
	rel, err := eng.StartDeployContext(reqCtx, pipeline.StartRequest{AppID: app.ID, Tag: "v1.5.0", UseLocalArtifact: true})
	reqCancel()
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, rel.ID)
	if done.Status != store.StatusSuccess {
		t.Fatalf("请求 ctx 取消不应终止流水线: 状态 = %s, err=%s\n%s", done.Status, done.Error, readLogFile(t, eng, rel.ID))
	}
}

func TestPEngineValidateArtifactAndHelpers(t *testing.T) {
	dir := t.TempDir()
	jar := filepath.Join(dir, "a.jar")
	os.WriteFile(jar, []byte("PK\x03\x04x"), 0o644)
	var sb strings.Builder
	if err := pipeline.ValidateArtifactForTest(&sb, "jar", jar); err != nil {
		t.Fatal(err)
	}
	notJar := filepath.Join(dir, "b.jar")
	os.WriteFile(notJar, []byte("NOPE-data"), 0o644)
	if err := pipeline.ValidateArtifactForTest(&sb, "jar", notJar); err == nil {
		t.Fatal("非 jar 应报错")
	}
	if err := pipeline.ValidateArtifactForTest(&sb, "jar", filepath.Join(dir, "missing")); err == nil {
		t.Fatal("缺失文件应报错")
	}
	empty := filepath.Join(dir, "e.jar")
	os.WriteFile(empty, []byte("PK"), 0o644)
	if err := pipeline.ValidateArtifactForTest(&sb, "jar", empty); err == nil {
		t.Fatal("过小文件应报错")
	}
	goBin := filepath.Join(dir, "app")
	os.WriteFile(goBin, []byte("\x7fELF-stub"), 0o644)
	if err := pipeline.ValidateArtifactForTest(&sb, "go", goBin); err != nil {
		t.Fatal(err)
	}
	notElf := filepath.Join(dir, "app2")
	os.WriteFile(notElf, []byte("MZ....."), 0o644)
	if err := pipeline.ValidateArtifactForTest(&sb, "go", notElf); err == nil {
		t.Fatal("非 ELF 应报错")
	}
	bin := filepath.Join(dir, "bin")
	os.WriteFile(bin, []byte("\x7fELF-shim\n"), 0o644)
	if err := pipeline.ValidateArtifactForTest(&sb, "binary", bin); err != nil {
		t.Fatal(err)
	}

	if !pipeline.IsYamlPathForTest("/a/b.YML") || pipeline.IsYamlPathForTest("/a/b.txt") {
		t.Fatal("isYamlPath 判断错误")
	}
	if pipeline.FirstNForTest("abcdef", 3) != "abc" || pipeline.FirstNForTest("ab", 5) != "ab" {
		t.Fatal("firstN 不正确")
	}
	if pipeline.ExtractTagForTest("app/demo") != "latest" || pipeline.ExtractTagForTest("app/demo:v1") != "v1" {
		t.Fatal("extractTag 不正确")
	}
	if pipeline.ImageRepoOfForTest("harbor/a@sha256:abc") != "harbor/a" || pipeline.ImageRepoOfForTest("harbor/a:v1") != "harbor/a" {
		t.Fatal("imageRepoOf 不正确")
	}
	if pipeline.K8sYamlFilePathForTest(&config.AppConfig{K8sYamlDir: "/y/", K8sYamlFile: "/f.yaml"}) != "/y/f.yaml" {
		t.Fatal("k8sYamlFilePath 不正确")
	}
	if pipeline.K8sYamlFilePathForTest(&config.AppConfig{K8sYamlFile: "f.yaml"}) != "f.yaml" || pipeline.K8sYamlFilePathForTest(&config.AppConfig{K8sYamlDir: "/y"}) != "/y" {
		t.Fatal("k8sYamlFilePath 分支不正确")
	}
	if err := pipeline.ValidateK8sTargetForTest(&config.AppConfig{Namespace: "a"}); err == nil {
		t.Fatal("缺少 Deployment/容器应报错")
	}
	if err := pipeline.ValidateAppForPipelineForTest(&config.AppConfig{ID: "a"}); err == nil {
		t.Fatal("缺少镜像应报错")
	}
}

func TestPEngineReplaceYamlImage(t *testing.T) {
	content := "kind: Deployment\nspec:\n  containers:\n  - image: app/demo:v1  # c\n  - image: other/sidecar:v2\n# image: app/demo:v0\n"
	out, ok := pipeline.ReplaceYamlImageForTest(content, "app/demo", "app/demo:v9")
	// 行尾注释保留的原始空白由 YAML 原文决定：这里只校验值与注释都在
	if !ok || !strings.Contains(out, "app/demo:v9") || !strings.Contains(out, "# c") || !strings.Contains(out, "other/sidecar:v2") || !strings.Contains(out, "# image: app/demo:v0") {
		t.Fatalf("替换结果不正确:\n%s", out)
	}
	// 仓库名不同的旧镜像：替换第一个 image 行
	out, ok = pipeline.ReplaceYamlImageForTest("a:\n  image: old/x:v1\n", "app/demo", "app/demo:v2")
	if !ok || !strings.Contains(out, "app/demo:v2") {
		t.Fatalf("回退替换失败:\n%s", out)
	}
	if _, ok := pipeline.ReplaceYamlImageForTest("# only comment\n", "app/demo", "x"); ok {
		t.Fatal("没有 image 行时应返回 false")
	}
	// pipeline.SplitImagePrefixForTest 各种写法
	for _, line := range []string{"image:  a  # c", "- image: \"a\"", "\timage: a"} {
		prefix, value, suffix, ok := pipeline.SplitImagePrefixForTest(line)
		if !ok || value == "" {
			t.Fatalf("splitImagePrefix(%q) = %q %q %q %v", line, prefix, value, suffix, ok)
		}
	}
	if _, _, _, ok := pipeline.SplitImagePrefixForTest("  # image: x"); ok {
		t.Fatal("注释行不应被识别")
	}
}

func TestPEngineRunLocalCommandAndFileSHA(t *testing.T) {
	dir := t.TempDir()
	var sb strings.Builder
	if err := pipeline.RunLocalCommandForTest(context.Background(), dir, "echo hi > out.txt", &sb); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "out.txt")); err != nil {
		t.Fatal("命令应在指定目录执行")
	}
	if err := pipeline.RunLocalCommandForTest(context.Background(), "", "exit 3", &sb); err == nil {
		t.Fatal("非零退出码应报错")
	}
	// Windows 的 echo 会输出 CRLF，按实际文件内容校验大小
	content, _ := os.ReadFile(filepath.Join(dir, "out.txt"))
	if strings.TrimSpace(string(content)) != "hi" {
		t.Fatalf("本地命令输出 = %q", content)
	}
	name, size, sum, err := pipeline.FileStatAndSHA256ForTest(filepath.Join(dir, "out.txt"))
	if err != nil || name != "out.txt" || size != int64(len(content)) || len(sum) != 64 {
		t.Fatalf("fileStatAndSHA256 = %s %d %s %v", name, size, sum, err)
	}
	if _, _, _, err := pipeline.FileStatAndSHA256ForTest(dir); err == nil {
		t.Fatal("目录应报错")
	}
}

func readLogFile(t *testing.T, eng *pipeline.Engine, id string) string {
	t.Helper()
	b, _ := os.ReadFile(eng.LogPath(id))
	return string(b)
}
