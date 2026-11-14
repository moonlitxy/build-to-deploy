package tests

import (
	"build-to-deploy/internal/api"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/k8s"
	"build-to-deploy/internal/pipeline"
	"build-to-deploy/internal/sshpool"
	"build-to-deploy/internal/store"
	"build-to-deploy/internal/upload"
)

// fakeRunner 模拟远程命令执行，记录命令并按关键字返回输出
type fakeRunner struct {
	cmds    []string
	outputs map[string]string
	err     error
	// runErr / runOut 仅作用于 RunCommand：模拟远程命令失败并带回一段输出
	runErr error
	runOut string
}

func (f *fakeRunner) RunCommand(ctx context.Context, srv *sshpool.ServerConfig, cmd string, w io.Writer) error {
	f.cmds = append(f.cmds, cmd)
	if f.runOut != "" {
		_, _ = io.WriteString(w, f.runOut)
	}
	return f.runErr
}
func (f *fakeRunner) ExecuteOutput(ctx context.Context, srv *sshpool.ServerConfig, cmd string) (string, error) {
	f.cmds = append(f.cmds, cmd)
	for k, v := range f.outputs {
		if strings.Contains(cmd, k) {
			return v, nil
		}
	}
	return "", f.err
}
func (f *fakeRunner) ReadRemoteFile(*sshpool.ServerConfig, string) (string, error) { return "", nil }
func (f *fakeRunner) WriteRemoteFile(*sshpool.ServerConfig, string, string) error  { return nil }
func (f *fakeRunner) UploadFile(context.Context, *sshpool.ServerConfig, string, string, func(int64, int64)) error {
	return nil
}
func (f *fakeRunner) MarkSimulated(*sshpool.ServerConfig) {}

// newTestServer 构建一个全部走真实执行分支、远程由假 SSH 接管的完整接口层
func newTestServer(t *testing.T) (*api.Server, *config.Config, *fakeRunner, string) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	yaml := `
server:
  listen: "127.0.0.1:0"
  dataDir: ` + fmt.Sprintf("%q", filepath.Join(dir, "data")) + `
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
    image: "app/demo"
    artifactDir: ` + fmt.Sprintf("%q", filepath.Join(dir, "artifacts")) + `
    buildDir: ` + fmt.Sprintf("%q", filepath.Join(dir, "build")) + `
    k8sYamlDir: ` + fmt.Sprintf("%q", filepath.Join(dir, "yaml")) + `
    k8sYamlFile: "demo.yaml"
    namespace: "prod"
    deployment: "demo"
    container: "demo"
  - id: "demo-docker"
    name: "demo-docker"
    type: "jar"
    serverId: "s1"
    image: "app/demo"
    artifactDir: ` + fmt.Sprintf("%q", filepath.Join(dir, "artifacts")) + `
    buildDir: ` + fmt.Sprintf("%q", filepath.Join(dir, "build")) + `
    deployMode: "docker"
    dockerContainer: "demo"
    dockerPort: "18081:8080"
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o644); err != nil {
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
	eng := pipeline.NewEngine(cfg, st, ups, nil, k8s.NewClient(nil), cfg.Server.DataDir)
	fake := &fakeRunner{outputs: map[string]string{}}
	eng.SSH = fake
	srv := api.NewServer(cfg, eng, k8s.NewClient(fake), nil, fs.FS(nil))
	// 其余用例验证的是"已连接服务器"后的行为；连接闸门本身由 TestAPIConnModeGate 覆盖
	srv.SetConnectionEnabled(true)
	return srv.WithRunner(fake), cfg, fake, dir
}

func do(t *testing.T, h http.Handler, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Host = "127.0.0.1:8080"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// waitDone 等待发布进入终态，避免流水线还在后台写日志时临时目录已被删除
func waitAPIDone(t *testing.T, srv *api.Server, id string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range api.NewServerForTest(srv).Engine().Store.List("", 0) {
			if r.ID == id && r.Done() {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("发布 %s 未在 15s 内结束", id)
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("响应不是 JSON: %s", rec.Body.String())
	}
}

func TestAPIAppsAndServersEndpoints(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()

	rec := do(t, h, "GET", "/api/apps", "", nil)
	if rec.Code != 200 {
		t.Fatalf("GET /api/apps = %d", rec.Code)
	}

	body := `{"id":"newapp","name":"newapp","type":"go","serverId":"s1","image":"app/new","artifactDir":"/tmp/a","buildDir":"/tmp/b","deployMode":"docker","dockerContainer":"newapp","dockerPort":"1:2"}`
	rec = do(t, h, "POST", "/api/apps", body, nil)
	if rec.Code != 200 {
		t.Fatalf("POST /api/apps = %d %s", rec.Code, rec.Body.String())
	}
	// 非法配置（Docker 端口）应 400
	bad := `{"id":"bad","name":"bad","serverId":"s1","image":"app/x","artifactDir":"/tmp/a","buildDir":"/tmp/b","deployMode":"docker","dockerContainer":"ok","dockerPort":"eighty"}`
	if rec = do(t, h, "POST", "/api/apps", bad, nil); rec.Code != 400 {
		t.Fatalf("非法 docker 端口应 400，实际 %d", rec.Code)
	}
	if rec = do(t, h, "DELETE", "/api/apps/newapp", "", nil); rec.Code != 200 {
		t.Fatalf("DELETE 应用 = %d", rec.Code)
	}
	if rec = do(t, h, "GET", "/api/servers", "", nil); rec.Code != 200 {
		t.Fatalf("GET /api/servers = %d", rec.Code)
	}
}

func TestAPIDeployTargetEndpoint(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	h := srv.Handler()

	// K8s 形态：只返回形态，不查容器
	var out map[string]any
	decode(t, do(t, h, "GET", "/api/deploy-target?appId=demo", "", nil), &out)
	if out["deployMode"] != "k8s" {
		t.Fatalf("k8s 形态返回 %v", out)
	}

	// Docker 形态：模拟宿主机上容器在运行
	fake.outputs["docker inspect -f"] = "running app/demo:v1.0.0\n"
	decode(t, do(t, h, "GET", "/api/deploy-target?appId=demo-docker", "", nil), &out)
	if out["deployMode"] != "docker" || out["container"] != "demo" {
		t.Fatalf("docker 形态返回 %v", out)
	}
	// Docker 形态：真实查询宿主机上的容器状态
	if !strings.Contains(strings.Join(fake.cmds, "\n"), "docker inspect -f") {
		t.Fatalf("应执行远程 docker inspect: %v", fake.cmds)
	}
	if out["running"] != true || out["image"] != "app/demo:v1.0.0" {
		t.Fatalf("应识别容器运行中: %v", out)
	}

	if rec := do(t, h, "GET", "/api/deploy-target?appId=nope", "", nil); rec.Code != 404 {
		t.Fatalf("未知应用应 404，实际 %d", rec.Code)
	}
}

func TestAPIGuardHostAndOrigin(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()

	req := httptest.NewRequest("GET", "/api/apps", nil)
	req.Host = "evil.example.com"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("非本机 Host 应 403，实际 %d", rec.Code)
	}

	rec = do(t, h, "POST", "/api/apps", `{"id":"x"}`, map[string]string{"Origin": "http://evil.example.com"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("跨站写请求应 403，实际 %d", rec.Code)
	}
	rec = do(t, h, "POST", "/api/apps", `{"id":"x"}`, map[string]string{"Origin": "http://127.0.0.1:8080"})
	if rec.Code == http.StatusForbidden {
		t.Fatalf("同源请求不应被拒绝")
	}
	// GET 同样校验 Origin（DNS rebinding 下可能读到发布日志等敏感数据）
	if rec = do(t, h, "GET", "/api/releases", "", map[string]string{"Origin": "http://evil.example.com"}); rec.Code != http.StatusForbidden {
		t.Fatalf("跨站读请求应 403，实际 %d", rec.Code)
	}
}

func TestAPIInspectArtifactEndpoint(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()

	// 没有产物路径：返回空指纹而不是报错
	var out map[string]any
	rec := do(t, h, "POST", "/api/artifacts/inspect", `{"appId":"demo","tag":"v1"}`, nil)
	if rec.Code != 200 {
		t.Fatalf("inspect = %d %s", rec.Code, rec.Body.String())
	}
	decode(t, rec, &out)
	if out["fingerprint"] != "" {
		t.Fatalf("无产物时应为空指纹: %v", out)
	}

	// 产物存在：能算出指纹；历史相同则返回 previous
	app := api.NewServerForTest(srv).Config().FindApp("demo")
	dir := t.TempDir()
	art := filepath.Join(dir, "app.jar")
	os.WriteFile(art, []byte("PK\x03\x04x"), 0o644)
	if err := os.MkdirAll(app.BuildDir, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(app.BuildDir, "Dockerfile"), []byte("FROM x\nCOPY app.jar /a\n"), 0o644)
	rec = do(t, h, "POST", "/api/artifacts/inspect", fmt.Sprintf(`{"appId":"demo","tag":"v1","localArtifactPath":%q}`, art), nil)
	if rec.Code != 200 {
		t.Fatalf("inspect with path = %d %s", rec.Code, rec.Body.String())
	}
	decode(t, rec, &out)
	if out["fingerprint"] == "" || out["reason"] != nil {
		t.Fatalf("应算出指纹: %v", out)
	}

	if rec = do(t, h, "POST", "/api/artifacts/inspect", `{"appId":"nope"}`, nil); rec.Code != 404 {
		t.Fatalf("未知应用应 404，实际 %d", rec.Code)
	}
	if rec = do(t, h, "POST", "/api/artifacts/inspect", `not-json`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
}

func TestAPIStartReleaseAndConflict(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	h := srv.Handler()
	app := api.NewServerForTest(srv).Config().FindApp("demo")
	art := filepath.Join(t.TempDir(), "app.jar")
	os.WriteFile(art, []byte("PK\x03\x04x"), 0o644)

	rec := do(t, h, "POST", "/api/releases", fmt.Sprintf(`{"appId":"demo","tag":"v1.0.0","useLocalArtifact":true,"localArtifactPath":%q}`, art), nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("启动发布 = %d %s", rec.Code, rec.Body.String())
	}
	var rel store.Release
	decode(t, rec, &rel)
	if rel.Tag != "v1.0.0" || rel.Mode != config.DeployModeK8s {
		t.Fatalf("发布记录异常: %+v", rel)
	}
	waitAPIDone(t, srv, rel.ID)

	// 本地已有同 Tag 镜像 -> 409 冲突，并携带预检结果；force=true 再试则放行
	fake.outputs["docker images -q"] = "abcdef123456\n"
	rec = do(t, h, "POST", "/api/releases", fmt.Sprintf(`{"appId":"demo-docker","tag":"v1.0.1","useLocalArtifact":true,"localArtifactPath":%q}`, art), nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("同 Tag 存在应 409，实际 %d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, h, "POST", "/api/releases", fmt.Sprintf(`{"appId":"demo-docker","tag":"v1.0.1","useLocalArtifact":true,"force":true,"localArtifactPath":%q}`, art), nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("force 后应创建，实际 %d %s", rec.Code, rec.Body.String())
	}
	var forced store.Release
	decode(t, rec, &forced)
	waitAPIDone(t, srv, forced.ID)
	_ = app

	// 非法 Tag
	if rec = do(t, h, "POST", "/api/releases", `{"appId":"demo","tag":"bad tag"}`, nil); rec.Code != 400 {
		t.Fatalf("非法 Tag 应 400，实际 %d", rec.Code)
	}
}

func TestAPIReleasesListAndLog(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()
	app := api.NewServerForTest(srv).Config().FindApp("demo")
	art := filepath.Join(t.TempDir(), "app.jar")
	os.WriteFile(art, []byte("PK\x03\x04x"), 0o644)
	rec := do(t, h, "POST", "/api/releases", fmt.Sprintf(`{"appId":"demo","tag":"v1.0.0","useLocalArtifact":true,"localArtifactPath":%q}`, art), nil)
	var rel store.Release
	decode(t, rec, &rel)
	waitAPIDone(t, srv, rel.ID)
	_ = app

	if rec = do(t, h, "GET", "/api/releases?limit=10", "", nil); rec.Code != 200 || !bytes.Contains(rec.Body.Bytes(), []byte(rel.ID)) {
		t.Fatalf("发布列表应包含新记录: %d %s", rec.Code, rec.Body.String())
	}
	if rec = do(t, h, "GET", "/api/releases/"+rel.ID, "", nil); rec.Code != 200 {
		t.Fatalf("查询发布 = %d", rec.Code)
	}
	if rec = do(t, h, "GET", "/api/releases?app=bad$id", "", nil); rec.Code != 400 {
		t.Fatalf("非法 app 过滤应 400，实际 %d", rec.Code)
	}
	trav := httptest.NewRequest("GET", "/api/releases/..%2f..%2fetc", nil)
	trav.Host = "127.0.0.1:8080"
	recT := httptest.NewRecorder()
	h.ServeHTTP(recT, trav)
	if recT.Code < 400 {
		t.Fatalf("路径穿越应被拒绝，实际 %d", recT.Code)
	}
}

func TestAPIAPIUploadLifecycle(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()

	rec := do(t, h, "POST", "/api/uploads", `{"fileName":"app.jar","size":8}`, nil)
	if rec.Code != 200 && rec.Code != 201 {
		t.Fatalf("创建上传 = %d %s", rec.Code, rec.Body.String())
	}
	var up struct {
		ID string `json:"id"`
	}
	decode(t, rec, &up)

	req := httptest.NewRequest("PUT", "/api/uploads/"+up.ID+"?offset=0", bytes.NewReader(make([]byte, 8)))
	req.Host = "127.0.0.1:8080"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("写分片 = %d %s", rec.Code, rec.Body.String())
	}
	if rec = do(t, h, "GET", "/api/uploads/"+up.ID, "", nil); rec.Code != 200 {
		t.Fatalf("查询上传 = %d", rec.Code)
	}
	if rec = do(t, h, "POST", "/api/uploads/"+up.ID+"/complete", "", nil); rec.Code != 200 {
		t.Fatalf("完成上传 = %d %s", rec.Code, rec.Body.String())
	}

	// 非法文件名在上传创建阶段就被拒绝
	// 路径会被 filepath.Base 去掉，最终文件名仍合法；真正的非法名（含空格/分号）必须拒绝
	if rec = do(t, h, "POST", "/api/uploads", `{"fileName":"evil;rm -rf/.jar","size":8}`, nil); rec.Code != 400 {
		t.Fatalf("非法文件名应 400，实际 %d %s", rec.Code, rec.Body.String())
	}
}

func TestAPIServerConfigAndLocalArtifact(t *testing.T) {
	srv, cfg, _, dir := newTestServer(t)
	h := srv.Handler()

	// 新增服务器：口令会被落盘且接口只返回掩码
	body := `{"id":"s2","name":"备用机","host":"10.0.0.2","port":22,"user":"root","authType":"password","password":"secret"}`
	rec := do(t, h, "POST", "/api/servers", body, nil)
	if rec.Code != 200 {
		t.Fatalf("保存服务器 = %d %s", rec.Code, rec.Body.String())
	}
	b, _ := os.ReadFile(cfg.Server.DataDir + "/../config.yaml")
	if !bytes.Contains(b, []byte("s2")) {
		t.Fatalf("配置文件未写入新服务器")
	}
	if cfg.FindServer("s2") == nil {
		t.Fatal("内存配置应包含新服务器")
	}

	// 非法服务器（缺 host）应 400
	if rec = do(t, h, "POST", "/api/servers", `{"name":"x","host":"","user":"root"}`, nil); rec.Code != 400 {
		t.Fatalf("非法服务器应 400，实际 %d", rec.Code)
	}

	// 删除服务器（接口会主动断开该服务器的长连接，测试里没有真实连接池，这里只校验非法 ID 的分支）
	if rec = do(t, h, "DELETE", "/api/servers/bad%24id", "", nil); rec.Code != 400 {
		t.Fatalf("非法服务器 ID 应 400，实际 %d", rec.Code)
	}

	// 本地产物状态：未配置 / 已配置但不存在
	rec = do(t, h, "GET", "/api/apps/demo/local-artifact", "", nil)
	if rec.Code != 200 {
		t.Fatalf("本地产物状态 = %d", rec.Code)
	}
	var info map[string]any
	decode(t, rec, &info)
	if info["configured"] != false {
		t.Fatalf("未配置产物时应返回 configured=false: %v", info)
	}

	rec = do(t, h, "GET", "/api/config/backups", "", nil)
	if rec.Code != 200 {
		t.Fatalf("备份列表 = %d", rec.Code)
	}
	var backups []config.ConfigBackup
	decode(t, rec, &backups)
	if rec = do(t, h, "POST", "/api/config/restore", `{"name":"not-exist"}`, nil); rec.Code != 400 {
		t.Fatalf("恢复不存在的备份应 400，实际 %d", rec.Code)
	}
	_ = dir
}

func TestAPIPrecheckEndpoint(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	h := srv.Handler()
	fake.outputs["docker images -q"] = "abcdef\n"
	fake.outputs["get deployment"] = "app/demo:latest\n"
	var res pipeline.PrecheckResult
	rec := do(t, h, "POST", "/api/precheck", `{"appId":"demo","tag":"latest"}`, nil)
	if rec.Code != 200 {
		t.Fatalf("预检 = %d %s", rec.Code, rec.Body.String())
	}
	decode(t, rec, &res)
	if len(res.Conflicts) < 2 {
		t.Fatalf("应同时发现本地镜像与线上同版本: %+v", res.Conflicts)
	}
	if rec = do(t, h, "POST", "/api/precheck", `{"appId":"demo","tag":"bad tag"}`, nil); rec.Code != 400 {
		t.Fatalf("非法 Tag 应 400，实际 %d", rec.Code)
	}
	if rec = do(t, h, "POST", "/api/precheck", `{"appId":"nope","tag":"v1"}`, nil); rec.Code != 404 {
		t.Fatalf("未知应用应 404，实际 %d", rec.Code)
	}
}

// 服务启动后默认不连接服务器：远程相关接口一律 412，打开开关后恢复
func TestAPIConnModeGate(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	srv.SetConnectionEnabled(false)
	h := srv.Handler()

	if rec := do(t, h, "GET", "/api/conn-mode", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "false") {
		t.Fatalf("默认应为未连接: %d %s", rec.Code, rec.Body.String())
	}
	remoteCalls := []struct{ method, path, body string }{
		{"GET", "/api/deploy-target?appId=demo", ""},
		{"POST", "/api/precheck", `{"appId":"demo","tag":"v1"}`},
		{"GET", "/api/k8s/namespaces?serverId=s1", ""},
		{"GET", "/api/k8s/pods?serverId=s1", ""},
		{"GET", "/api/k8s/pod-logs?serverId=s1&pod=p1", ""},
		{"POST", "/api/k8s/restart", `{"serverId":"s1","namespace":"shuili","deployment":"demo"}`},
		{"POST", "/api/releases", `{"appId":"demo","tag":"v1","useLocalArtifact":true}`},
	}
	for _, c := range remoteCalls {
		if rec := do(t, h, c.method, c.path, c.body, nil); rec.Code != 412 {
			t.Fatalf("%s %s 未连接时应 412，实际 %d %s", c.method, c.path, rec.Code, rec.Body.String())
		}
	}
	// 配置类只读接口不受影响：无需连接也能查看/编辑配置
	if rec := do(t, h, "GET", "/api/apps", "", nil); rec.Code != 200 {
		t.Fatalf("应用列表应在离线时可读: %d", rec.Code)
	}

	// 打开开关后恢复
	if rec := do(t, h, "POST", "/api/conn-mode", `{"enabled":true}`, nil); rec.Code != 200 {
		t.Fatalf("开启 = %d %s", rec.Code, rec.Body.String())
	}
	if !srv.ConnectionEnabled() {
		t.Fatal("开关应已打开")
	}
	fake.outputs["kubectl get namespaces"] = "shuili"
	if rec := do(t, h, "GET", "/api/k8s/namespaces?serverId=s1", "", nil); rec.Code != 200 {
		t.Fatalf("已连接后命名空间 = %d %s", rec.Code, rec.Body.String())
	}

	// 关闭开关：同时断开所有长连接，并再次拦截远程接口
	if rec := do(t, h, "POST", "/api/conn-mode", `{"enabled":false}`, nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "closed") {
		t.Fatalf("关闭 = %d %s", rec.Code, rec.Body.String())
	}
	if srv.ConnectionEnabled() {
		t.Fatal("开关应已关闭")
	}
	if rec := do(t, h, "GET", "/api/k8s/namespaces?serverId=s1", "", nil); rec.Code != 412 {
		t.Fatalf("关闭后应 412，实际 %d", rec.Code)
	}
	// 非法 body 返回 400
	if rec := do(t, h, "POST", "/api/conn-mode", `{`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
}

func TestAPIServerConnectionEndpointsWithoutPool(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()

	// 从未建立过真实连接池：这些接口应安全返回而不是 panic
	if rec := do(t, h, "POST", "/api/servers/test", `{"id":"s1"}`, nil); rec.Code != 500 {
		t.Fatalf("无连接池测试连接应返回 500，实际 %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, "POST", "/api/servers/disconnect", `{"serverId":"all"}`, nil); rec.Code != 200 {
		t.Fatalf("断开连接 = %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/servers/connections", "", nil); rec.Code != 200 {
		t.Fatalf("连接列表 = %d", rec.Code)
	}
	if rec := do(t, h, "DELETE", "/api/servers/s1", "", nil); rec.Code != 200 {
		t.Fatalf("删除服务器 = %d %s", rec.Code, rec.Body.String())
	}
}

func TestAPIReleaseLogCancelAndNotFound(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()
	art := filepath.Join(t.TempDir(), "app.jar")
	os.WriteFile(art, []byte("PK\x03\x04x"), 0o644)
	rec := do(t, h, "POST", "/api/releases", fmt.Sprintf(`{"appId":"demo","tag":"v3.0.0","useLocalArtifact":true,"localArtifactPath":%q}`, art), nil)
	var rel store.Release
	decode(t, rec, &rel)
	waitAPIDone(t, srv, rel.ID)

	if rec = do(t, h, "GET", "/api/releases/"+rel.ID+"/log", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "开始发布应用") {
		t.Fatalf("日志下载 = %d %s", rec.Code, rec.Body.String())
	}
	if rec = do(t, h, "GET", "/api/releases/missing/log", "", nil); rec.Code != 404 {
		t.Fatalf("不存在发布的日志应 404，实际 %d", rec.Code)
	}
	if rec = do(t, h, "POST", "/api/releases/missing/cancel", "", nil); rec.Code != 400 {
		t.Fatalf("取消不存在发布应 400，实际 %d", rec.Code)
	}
	// 同一应用并发锁：另起一件不同 Tag 的发布（或其回滚）应被拒绝
	// 记录不存在属于状态冲突（409），与参数错误区分，页面可给出准确提示
	if rec = do(t, h, "POST", "/api/releases/missing/rollback", `{}`, nil); rec.Code != 409 {
		t.Fatalf("不存在发布的回滚应 409，实际 %d", rec.Code)
	}
	// 刚结束的发布记录可能仍被流水线 goroutine 收尾（并发的同名应用会被锁拒绝），两种结果都合理
	rbRec := do(t, h, "POST", "/api/releases/"+rel.ID+"/rollback", `{"remark":"手动回滚"}`, nil)
	if rbRec.Code == http.StatusAccepted {
		var rb store.Release
		decode(t, rbRec, &rb)
		waitAPIDone(t, srv, rb.ID)
	} else if rbRec.Code != 400 && rbRec.Code != 409 {
		// 409：目标镜像已被清理（假执行器没有"镜像存在"规则时即如此）
		t.Fatalf("回滚结果异常: %d %s", rbRec.Code, rbRec.Body.String())
	}
}

func TestAPIPodLogsAndRestartValidation(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()
	if rec := do(t, h, "GET", "/api/k8s/pod-logs?serverId=s1&namespace=shuili&pod=BAD_POD", "", nil); rec.Code != 400 {
		t.Fatalf("非法 Pod 名应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/k8s/pod-logs?serverId=s1&namespace=shuili&pod=demo-1&container=bad%20c", "", nil); rec.Code != 400 {
		t.Fatalf("非法容器名应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/k8s/pods?serverId=s1&namespace=BAD", "", nil); rec.Code != 400 {
		t.Fatalf("非法命名空间应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/k8s/restart", `not-json`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
}

func TestAPIInspectLocalArtifactBranches(t *testing.T) {
	srv, cfg, _, _ := newTestServer(t)
	h := srv.Handler()
	art := filepath.Join(t.TempDir(), "app.jar")
	os.WriteFile(art, []byte("x"), 0o644)
	app := *api.NewServerForTest(srv).Config().FindApp("demo")
	app.LocalArtifactPath = art
	app.LocalBuildCmd = "mvn package"
	cfg.UpdateApp(&app)
	var info map[string]any
	rec := do(t, h, "GET", "/api/apps/demo/local-artifact", "", nil)
	decode(t, rec, &info)
	if info["configured"] != true || info["exists"] != true {
		t.Fatalf("本地产物已就绪: %v", info)
	}
	if rec = do(t, h, "GET", "/api/apps/bad%20id/local-artifact", "", nil); rec.Code != 400 {
		t.Fatalf("非法 ID 应 400，实际 %d", rec.Code)
	}
	if rec = do(t, h, "GET", "/api/apps/missing/local-artifact", "", nil); rec.Code != 404 {
		t.Fatalf("未知应用应 404，实际 %d", rec.Code)
	}
}

func TestAPIK8sEndpoints(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	h := srv.Handler()

	fake.outputs["kubectl get namespaces"] = "shuili prod"
	if rec := do(t, h, "GET", "/api/k8s/namespaces?serverId=s1", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "shuili") {
		t.Fatalf("命名空间 = %d %s", rec.Code, rec.Body.String())
	}
	fake.outputs["kubectl get pods"] = `{"items":[{"metadata":{"name":"p1","namespace":"shuili"},"spec":{"containers":[{"name":"c","image":"app:v1"}]},"status":{"phase":"Running"}}]}`
	if rec := do(t, h, "GET", "/api/k8s/pods?serverId=s1&namespace=shuili", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "p1") {
		t.Fatalf("Pod 列表 = %d %s", rec.Code, rec.Body.String())
	}
	fake.outputs["kubectl logs"] = "hello log"
	if rec := do(t, h, "GET", "/api/k8s/pod-logs?serverId=s1&namespace=shuili&pod=p1&container=c&tail=10", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "hello log") {
		t.Fatalf("Pod 日志 = %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, "POST", "/api/k8s/restart", `{"serverId":"s1","namespace":"shuili","deployment":"demo"}`, nil); rec.Code != 200 {
		t.Fatalf("重启 = %d %s", rec.Code, rec.Body.String())
	}
	// 远端失败：500（先清空成功规则，避免按关键字命中）
	fake.outputs = map[string]string{}
	fake.err = fmt.Errorf("ssh boom")
	if rec := do(t, h, "GET", "/api/k8s/namespaces?serverId=s1", "", nil); rec.Code != 500 {
		t.Fatalf("远端失败应 500，实际 %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/k8s/pods?serverId=s1", "", nil); rec.Code != 500 {
		t.Fatalf("Pod 远端失败应 500，实际 %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/k8s/pod-logs?serverId=s1&pod=p1", "", nil); rec.Code != 500 {
		t.Fatalf("日志远端失败应 500，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/k8s/restart", `{"serverId":"s1","namespace":"shuili","deployment":"demo"}`, nil); rec.Code != 500 {
		t.Fatalf("重启远端失败应 500，实际 %d", rec.Code)
	}
}

func TestAPIReleaseEventsWithLiveHub(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()
	art := filepath.Join(t.TempDir(), "app.jar")
	os.WriteFile(art, []byte("PK\x03\x04x"), 0o644)
	// 启动后立即订阅：此时 Hub 仍存活，走实时推送分支
	rec := do(t, h, "POST", "/api/releases", fmt.Sprintf(`{"appId":"demo","tag":"v5.0.0","useLocalArtifact":true,"localArtifactPath":%q}`, art), nil)
	var rel store.Release
	decode(t, rec, &rel)

	ts := httptest.NewServer(h)
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"/api/releases/"+rel.ID+"/events", nil)
	req.Host = "127.0.0.1:8080"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	if n == 0 || !strings.Contains(string(buf[:n]), "event: release") {
		t.Fatalf("应收到 release 事件: %q", string(buf[:n]))
	}
	waitAPIDone(t, srv, rel.ID)
}

// 发布页展示线上当前版本：K8s / Docker 两种形态都能返回 Tag，读不到时不报错而是返回空字段
func TestAPICurrentVersion(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	h := srv.Handler()
	var got struct {
		Image string `json:"image"`
		Tag   string `json:"tag"`
	}

	fake.outputs["get deployment"] = "app/demo:v1.2.3"
	rec := do(t, h, "GET", "/api/apps/demo/current-version", "", nil)
	if rec.Code != 200 {
		t.Fatalf("当前版本 = %d %s", rec.Code, rec.Body.String())
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.Tag != "v1.2.3" || got.Image != "app/demo:v1.2.3" {
		t.Fatalf("K8s 当前版本解析错误: %+v", got)
	}

	fake.outputs = map[string]string{"docker inspect": "running app/demo:v2.0.0"}
	rec = do(t, h, "GET", "/api/apps/demo-docker/current-version", "", nil)
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.Tag != "v2.0.0" {
		t.Fatalf("Docker 当前版本解析错误: %+v", got)
	}

	// 读不到（远端报错）时返回空字段而不是 5xx，页面提示"暂时无法读取"
	fake.outputs = map[string]string{}
	fake.err = errFromString("ssh 断开")
	rec = do(t, h, "GET", "/api/apps/demo/current-version", "", nil)
	if rec.Code != 200 {
		t.Fatalf("读取失败应降级为 200，实际 %d %s", rec.Code, rec.Body.String())
	}
	var empty map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &empty)
	if empty["tag"] != "" {
		t.Fatalf("读取失败时 tag 应为空: %+v", empty)
	}

	if rec := do(t, h, "GET", "/api/apps/nope/current-version", "", nil); rec.Code != 404 {
		t.Fatalf("未知应用应 404，实际 %d", rec.Code)
	}
	// 未连接服务器时不访问远程
	srv.SetConnectionEnabled(false)
	if rec := do(t, h, "GET", "/api/apps/demo/current-version", "", nil); rec.Code != 412 {
		t.Fatalf("未连接时应 412，实际 %d", rec.Code)
	}
}

func errFromString(s string) error { return errors.New(s) }
