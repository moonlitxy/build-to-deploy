package yamlbackup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"build-to-deploy/internal/api"
	"build-to-deploy/internal/config"
	"build-to-deploy/internal/k8s"
	"build-to-deploy/internal/pipeline"
	"build-to-deploy/internal/shellrun"
	"build-to-deploy/internal/sshpool"
	"build-to-deploy/internal/store"
	"build-to-deploy/internal/upload"
)

// fakeSSH 用 /tmp 下的目录模拟远程服务器：文件读写真实落盘，命令执行用规则返回。
type fakeSSH struct {
	mu           sync.Mutex
	files        map[string]string
	cmds         []string
	rules        []fakeRule
	execErr      error
	failContains string
}

type fakeRule struct {
	contains string
	out      string
}

func (f *fakeSSH) RunCommand(ctx context.Context, srv *sshpool.ServerConfig, cmd string, w io.Writer) error {
	f.mu.Lock()
	f.cmds = append(f.cmds, cmd)
	fail := f.failContains != "" && strings.Contains(cmd, f.failContains)
	err := f.execErr
	f.mu.Unlock()
	if fail {
		return errors.New("模拟部署命令失败")
	}
	return err
}

func (f *fakeSSH) ExecuteOutput(ctx context.Context, srv *sshpool.ServerConfig, cmd string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = append(f.cmds, cmd)
	if f.execErr != nil {
		return "", f.execErr
	}
	for _, r := range f.rules {
		if strings.Contains(cmd, r.contains) {
			return r.out, nil
		}
	}
	return "", nil
}

func (f *fakeSSH) ReadRemoteFile(srv *sshpool.ServerConfig, p string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.files[p]; ok {
		return v, nil
	}
	return "", errors.New("missing")
}

func (f *fakeSSH) WriteRemoteFile(srv *sshpool.ServerConfig, p, content string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.files == nil {
		f.files = map[string]string{}
	}
	f.files[p] = content
	return nil
}

func (f *fakeSSH) UploadFile(ctx context.Context, srv *sshpool.ServerConfig, local, remote string, fn func(int64, int64)) error {
	return nil
}

func (f *fakeSSH) file(p string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.files[p]
}

func (f *fakeSSH) setRule(contains, out string) {
	f.mu.Lock()
	f.rules = append(f.rules, fakeRule{contains: contains, out: out})
	f.mu.Unlock()
}

func (f *fakeSSH) setFailContains(s string) {
	f.mu.Lock()
	f.failContains = s
	f.mu.Unlock()
}

func (f *fakeSSH) hasCmd(sub string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.cmds {
		if strings.Contains(c, sub) {
			return true
		}
	}
	return false
}

type env struct {
	dir       string
	remoteDir string
	yamlPath  string
	cfg       *config.Config
	engine    *pipeline.Engine
	store     *store.Store
	fake      *fakeSSH
	app       *config.AppConfig
	server    *sshpool.ServerConfig
}

// newEnv 在 /tmp 下创建独立的测试根目录，远程目录与本地备份目录分开。
func newEnv(t *testing.T) *env {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "btd-yamlbackup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	remoteDir := filepath.Join(dir, "remote")
	for _, d := range []string{remoteDir, filepath.Join(dir, "data"), filepath.Join(dir, "artifacts"), filepath.Join(dir, "build")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	yamlPath := filepath.Join(remoteDir, "demo.yaml")
	if err := os.WriteFile(yamlPath, []byte(originalYaml), 0o644); err != nil {
		t.Fatal(err)
	}

	cfgText := `
server:
  listen: "127.0.0.1:0"
  dataDir: ` + q(filepath.Join(dir, "data")) + `
  yamlBackupDir: ` + q(filepath.Join(dir, "yaml-backups")) + `
  yamlBackupKeep: 30
  yamlBackupMaxAgeDays: 30
sshServers:
  - id: s1
    name: 测试服务器
    host: 127.0.0.1
    port: 22
    user: root
apps:
  - id: demo
    name: demo
    type: jar
    serverId: s1
    image: app/demo
    artifactDir: ` + q(filepath.Join(dir, "artifacts")) + `
    buildDir: ` + q(filepath.Join(dir, "build")) + `
    k8sYamlDir: ` + q(remoteDir) + `
    k8sYamlFile: demo.yaml
    namespace: prod
    deployment: demo
    container: demo
    deployCmd: "kubectl apply -f {{.K8sYamlPath}}"
`
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfgText), 0o644); err != nil {
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
	fake := &fakeSSH{files: map[string]string{yamlPath: originalYaml}}
	eng := pipeline.NewEngine(cfg, st, ups, nil, k8s.NewClient(fake), cfg.Server.DataDir)
	eng.SSH = fake
	return &env{dir: dir, remoteDir: remoteDir, yamlPath: yamlPath, cfg: cfg, engine: eng, store: st, fake: fake, app: cfg.Apps[0], server: cfg.SSHServers[0]}
}

const originalYaml = "kind: Deployment\nspec:\n  template:\n    spec:\n      containers:\n      - name: demo\n        image: app/demo:v0.9.0  # 保留注释\n"

func q(s string) string { return strconv.Quote(filepath.ToSlash(s)) }

func (e *env) runner(t *testing.T, relID, tag, image, ip string) *pipeline.RunnerLike {
	t.Helper()
	hub := pipeline.NewHub(filepath.Join(e.dir, "logs", relID+".log"))
	t.Cleanup(hub.Close)
	rel := &store.Release{ID: relID, App: e.app.ID, Kind: "deploy", Tag: tag, Image: image, OperatorIP: ip, Status: store.StatusRunning}
	return pipeline.NewRunnerLike(e.engine, context.Background(), rel, e.app, e.server, hub, "", nil)
}

func (e *env) backupDir() string {
	return filepath.Join(e.cfg.ServerSettingsSnapshot().YamlBackupDir, e.app.ID)
}

func countYaml(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".yaml") {
			n++
		}
	}
	return n
}

func TestSnapshotRecordsMetadataAndFailsClosed(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t, "rel-1", "v1.0.0", "app/demo:v1.0.0", "127.0.0.1")
	if err := pipeline.UpdateRemoteYamlForTest(r, io.Discard, "app/demo:v1.0.0"); err != nil {
		t.Fatal(err)
	}
	if got := e.fake.file(e.yamlPath); !strings.Contains(got, "app/demo:v1.0.0") {
		t.Fatalf("远程 YAML 未更新: %s", got)
	}
	entries, _ := os.ReadDir(e.backupDir())
	var metaPath string
	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), ".json") {
			metaPath = filepath.Join(e.backupDir(), ent.Name())
		}
	}
	if metaPath == "" {
		t.Fatal("应生成快照元信息")
	}
	b, _ := os.ReadFile(metaPath)
	var meta pipeline.YamlBackupMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		t.Fatal(err)
	}
	if meta.AppID != "demo" || meta.ServerID != "s1" || meta.ServerHost != "127.0.0.1" ||
		meta.YamlPath != e.yamlPath || meta.TargetImage != "app/demo:v1.0.0" ||
		meta.PreviousImage != "app/demo:v0.9.0" || meta.OperatorIP != "127.0.0.1" || meta.Tag != "v1.0.0" {
		t.Fatalf("快照元信息不完整: %+v", meta)
	}
	content, err := os.ReadFile(filepath.Join(e.backupDir(), meta.Name))
	if err != nil || !strings.Contains(string(content), "app/demo:v0.9.0") {
		t.Fatalf("快照内容应为修改前 YAML: %s %v", content, err)
	}

	// 备份目录不可创建时必须中止，且远程文件保持原样
	blocker := filepath.Join(e.dir, "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.cfg.Server.YamlBackupDir = filepath.Join(blocker, "sub")
	e.fake.WriteRemoteFile(e.server, e.yamlPath, originalYaml)
	before := e.fake.file(e.yamlPath)
	r2 := e.runner(t, "rel-2", "v2.0.0", "app/demo:v2.0.0", "127.0.0.1")
	if err := pipeline.UpdateRemoteYamlForTest(r2, io.Discard, "app/demo:v2.0.0"); err == nil {
		t.Fatal("备份失败时应中止发布")
	}
	if after := e.fake.file(e.yamlPath); after != before {
		t.Fatalf("备份失败后不应改动远程 YAML: %s", after)
	}
}

func TestBackupRetentionByCountAndAge(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 35; i++ {
		id := "rel-" + strconv.Itoa(i)
		tag := "v" + strconv.Itoa(i) + ".0.0"
		r := e.runner(t, id, tag, "app/demo:"+tag, "127.0.0.1")
		if err := pipeline.UpdateRemoteYamlForTest(r, io.Discard, "app/demo:"+tag); err != nil {
			t.Fatal(err)
		}
	}
	if n := countYaml(t, e.backupDir()); n != 30 {
		t.Fatalf("按份数应保留 30 份，实际 %d", n)
	}

	// 造一份很旧的备份，并关掉份数限制，只按天数清理
	oldName := "old-release_app-demo:v0.1.0"
	oldBase := filepath.Join(e.backupDir(), oldName)
	if err := os.WriteFile(oldBase+".yaml", []byte("kind: Deployment\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldMeta := pipeline.YamlBackupMeta{Name: oldName + ".yaml", AppID: e.app.ID, CreatedAt: time.Now().Add(-40 * 24 * time.Hour)}
	b, _ := json.Marshal(oldMeta)
	if err := os.WriteFile(oldBase+".json", b, 0o600); err != nil {
		t.Fatal(err)
	}
	e.cfg.Server.YamlBackupKeep = -1
	e.cfg.Server.YamlBackupMaxAgeDays = 30
	r := e.runner(t, "rel-new", "v99.0.0", "app/demo:v99.0.0", "127.0.0.1")
	if err := pipeline.UpdateRemoteYamlForTest(r, io.Discard, "app/demo:v99.0.0"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldBase + ".yaml"); !os.IsNotExist(err) {
		t.Fatal("超期备份应被清理")
	}
}

func TestListAndReadBackups(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t, "rel-list", "v1.2.3", "app/demo:v1.2.3", "::1")
	if err := pipeline.UpdateRemoteYamlForTest(r, io.Discard, "app/demo:v1.2.3"); err != nil {
		t.Fatal(err)
	}
	list, err := pipeline.ListYamlBackups(e.cfg)
	if err != nil || len(list) != 1 {
		t.Fatalf("列表 = %+v %v", list, err)
	}
	if list[0].AppName != "demo" || list[0].ServerHost != "127.0.0.1" || list[0].OperatorIP != "::1" {
		t.Fatalf("列表缺少归属信息: %+v", list[0])
	}
	meta, content, err := pipeline.ReadYamlBackup(e.cfg, "demo", list[0].Name)
	if err != nil || !strings.Contains(content, "app/demo:v0.9.0") || meta.ServerID != "s1" {
		t.Fatalf("读取备份 = %+v %q %v", meta, content, err)
	}
	for _, name := range []string{"../demo.yaml", "a/b.yaml", "demo.txt"} {
		if _, _, err := pipeline.ReadYamlBackup(e.cfg, "demo", name); err == nil {
			t.Fatalf("非法文件名应被拒绝: %s", name)
		}
	}
}

func (e *env) waitRelease(t *testing.T, id string) *store.Release {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rel, err := e.store.Get(id)
		if err == nil && rel.Done() {
			return rel
		}
		time.Sleep(20 * time.Millisecond)
	}
	rel, _ := e.store.Get(id)
	t.Fatalf("恢复任务超时: %+v", rel)
	return nil
}

func TestRestoreFileOnlyAndApplyFailureReverts(t *testing.T) {
	e := newEnv(t)
	// 先发布一次，拿到 v0.9 -> v1.0 的快照
	r := e.runner(t, "rel-deploy", "v1.0.0", "app/demo:v1.0.0", "127.0.0.1")
	if err := pipeline.UpdateRemoteYamlForTest(r, io.Discard, "app/demo:v1.0.0"); err != nil {
		t.Fatal(err)
	}
	list, _ := pipeline.ListYamlBackups(e.cfg)
	if len(list) != 1 {
		t.Fatalf("应有 1 份快照: %+v", list)
	}
	backupName := list[0].Name

	// 只恢复文件：远程回到 v0.9，且不执行 apply
	rel, err := e.engine.StartYamlRestore(pipeline.YamlRestoreRequest{AppID: "demo", Name: backupName, Apply: false, ClientIP: "10.0.0.9"})
	if err != nil {
		t.Fatal(err)
	}
	done := e.waitRelease(t, rel.ID)
	if done.Status != store.StatusSuccess {
		t.Fatalf("只恢复文件失败: %s %s", done.Status, done.Error)
	}
	if got := e.fake.file(e.yamlPath); !strings.Contains(got, "app/demo:v0.9.0") {
		t.Fatalf("应写回快照内容: %s", got)
	}
	if done.OperatorIP != "10.0.0.9" {
		t.Fatalf("应记录来源 IP: %+v", done)
	}
	if n := countYaml(t, e.backupDir()); n != 2 {
		t.Fatalf("恢复前应再存一份快照，实际 %d", n)
	}

	// 恢复并发布成功：写回文件后执行部署命令并等待就绪
	rel3, err := e.engine.StartYamlRestore(pipeline.YamlRestoreRequest{AppID: "demo", Name: backupName, Apply: true, ClientIP: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	if done3 := e.waitRelease(t, rel3.ID); done3.Status != store.StatusSuccess {
		t.Fatalf("恢复并发布失败: %s %s", done3.Status, done3.Error)
	}
	if !e.fake.hasCmd("kubectl apply -f") || !e.fake.hasCmd("rollout status") {
		t.Fatal("恢复并发布应执行部署命令并等待就绪")
	}

	// 恢复并发布：apply 失败时必须把文件退回恢复前内容
	e.fake.WriteRemoteFile(e.server, e.yamlPath, "kind: Deployment\nspec:\n  template:\n    spec:\n      containers:\n      - name: demo\n        image: app/demo:v2.0.0\n")
	e.fake.setFailContains("kubectl apply")
	rel2, err := e.engine.StartYamlRestore(pipeline.YamlRestoreRequest{AppID: "demo", Name: backupName, Apply: true, ClientIP: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	done2 := e.waitRelease(t, rel2.ID)
	if done2.Status != store.StatusFailed {
		t.Fatalf("apply 失败应标记失败: %s", done2.Status)
	}
	if got := e.fake.file(e.yamlPath); !strings.Contains(got, "app/demo:v2.0.0") {
		t.Fatalf("失败后应退回恢复前内容: %s", got)
	}
}

func TestValidateAppRejectsDirOnlyApply(t *testing.T) {
	base := config.AppConfig{ID: "demo", Name: "demo", Type: "jar", ArtifactDir: "/data/a", BuildDir: "/data/b", K8sYamlDir: "/data/k8s", Image: "app/demo", DeployMode: config.DeployModeK8s}
	bad := base
	bad.DeployCmd = "kubectl apply -f {{.K8sYamlDir}}"
	if err := config.ValidateApp(&bad); err == nil {
		t.Fatal("只填目录 + apply 目录应被拒绝")
	}
	good := base
	good.K8sYamlFile = "demo.yaml"
	good.DeployCmd = "kubectl apply -f {{.K8sYamlPath}}"
	if err := config.ValidateApp(&good); err != nil {
		t.Fatalf("具体文件模式不应被拒绝: %v", err)
	}
	setImage := base
	setImage.DeployCmd = "kubectl -n prod set image deployment/demo demo=app/demo:{{.Tag}}"
	if err := config.ValidateApp(&setImage); err != nil {
		t.Fatalf("自管镜像命令不应被拒绝: %v", err)
	}
}

func TestAPIYamlBackupEndpoints(t *testing.T) {
	e := newEnv(t)
	r := e.runner(t, "rel-api", "v3.0.0", "app/demo:v3.0.0", "127.0.0.1")
	if err := pipeline.UpdateRemoteYamlForTest(r, io.Discard, "app/demo:v3.0.0"); err != nil {
		t.Fatal(err)
	}
	kube := k8s.NewClient(e.fake)
	srv := api.NewServer(e.cfg, e.engine, kube, nil, fstest.MapFS{}).WithRunner(e.fake)
	srv.SetConnectionEnabled(true)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	res, err := http.Get(ts.URL + "/api/config/yaml-backups")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var list []pipeline.YamlBackupMeta
	if res.StatusCode != http.StatusOK || json.NewDecoder(res.Body).Decode(&list) != nil || len(list) != 1 {
		t.Fatalf("列表接口 = %d %+v", res.StatusCode, list)
	}

	dl := ts.URL + "/api/config/yaml-backups/download?appId=demo&name=" + list[0].Name
	dres, err := http.Get(dl)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(dres.Body)
	dres.Body.Close()
	if dres.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("app/demo:v0.9.0")) {
		t.Fatalf("下载接口 = %d %s", dres.StatusCode, body)
	}

	bad, err := http.Get(ts.URL + "/api/config/yaml-backups/download?appId=demo&name=../x.yaml")
	if err != nil {
		t.Fatal(err)
	}
	bad.Body.Close()
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("路径穿越应返回 400，实际 %d", bad.StatusCode)
	}

	payload, _ := json.Marshal(map[string]any{"appId": "demo", "name": list[0].Name, "apply": false})
	rres, err := http.Post(ts.URL+"/api/config/yaml-backups/restore", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var rel store.Release
	_ = json.NewDecoder(rres.Body).Decode(&rel)
	rres.Body.Close()
	if rres.StatusCode != http.StatusAccepted || rel.ID == "" {
		t.Fatalf("恢复接口 = %d %+v", rres.StatusCode, rel)
	}
	if done := e.waitRelease(t, rel.ID); done.Status != store.StatusSuccess {
		t.Fatalf("接口触发的恢复失败: %s", done.Status)
	}

	// 未连接服务器时恢复必须被拒绝
	off := api.NewServer(e.cfg, e.engine, kube, nil, fstest.MapFS{}).WithRunner(e.fake)
	offTS := httptest.NewServer(off.Handler())
	defer offTS.Close()
	resp, err := http.Post(offTS.URL+"/api/config/yaml-backups/restore", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("未连接应拒绝恢复，实际 %d", resp.StatusCode)
	}
}

// 任意历史版本都能回滚，不只限制最近一条记录。
func TestRollbackAcceptsHistoricalRelease(t *testing.T) {
	e := newEnv(t)
	e.fake.setRule("docker image inspect", "0")
	for _, tag := range []string{"v0.1.0", "v0.2.0"} {
		if err := e.store.Add(&store.Release{
			ID: "rel-" + tag, App: "demo", Kind: "deploy", Tag: tag, Image: "app/demo:" + tag,
			Status: store.StatusSuccess, CreatedAt: time.Now().Add(-time.Hour), Stages: []store.Stage{},
		}); err != nil {
			t.Fatal(err)
		}
	}
	rel, err := e.engine.StartRollback(pipeline.RollbackRequest{ReleaseID: "rel-v0.1.0", ClientIP: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	done := e.waitRelease(t, rel.ID)
	if done.Status != store.StatusSuccess || done.Tag != "v0.1.0" || done.OperatorIP != "127.0.0.1" {
		t.Fatalf("历史版本回滚 = %+v", done)
	}
	if got := e.fake.file(e.yamlPath); !strings.Contains(got, "app/demo:v0.1.0") {
		t.Fatalf("YAML 应回滚到历史镜像: %s", got)
	}
}

var _ shellrun.Runner = (*fakeSSH)(nil)
var _ fs.FS = fstest.MapFS{}
