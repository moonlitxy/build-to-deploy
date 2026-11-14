package tests

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/pipeline"
	"build-to-deploy/internal/store"
)

// 直接驱动部署前一致性确认的各条分支（无需跑完整流水线）
func TestPConsistencyBranches(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	hub := pipeline.NewHub(filepath.Join(t.TempDir(), "c.log"))
	defer hub.Close()

	newRunner := func(rel *store.Release) *pipeline.RunnerLike {
		r := pipeline.NewRunnerLike(eng, context.Background(), rel, app, eng.Cfg.SSHServers[0], hub, "", &pipeline.PrecheckResult{CurrentTag: "v0.9.0"})
		return r
	}

	// 1) 本次镜像摘要为空：跳过检查
	r := newRunner(&store.Release{ID: "r1", Tag: "v1.0.0", App: app.ID})
	if err := pipeline.ConfirmBeforeDeployForTest(r, true); err != nil {
		t.Fatalf("摘要为空时应跳过: %v", err)
	}

	// 2) 线上摘要取不到：跳过
	sum := "sha256:" + strings.Repeat("9", 64)
	fake.addRules(scriptRule{"get deployment", "app/demo:v0.9.0"}) // get pods 无规则 → 返回空
	st.Add(&store.Release{ID: "r2", Tag: "v1.0.0", App: app.ID, Status: store.StatusRunning})
	r = newRunner(&store.Release{ID: "r2", Tag: "v1.0.0", App: app.ID})
	r.SetImageSummaryForTest(sum)
	if err := pipeline.ConfirmBeforeDeployForTest(r, true); err != nil {
		t.Fatalf("线上摘要未知时应跳过: %v", err)
	}

	// 3) 线上摘要不同，但与历史版本相同：提示"只是换了 Tag"
	other := "sha256:" + strings.Repeat("a", 64)
	fake.addRules(scriptRule{"get pods", "docker://" + other + "\n"})
	// scriptedRunner 的规则按添加顺序命中，"docker image inspect" 可能先于 get pods 匹配，改成整体替换
	fake.rules = nil
	fake.addRules(
		scriptRule{"get pods", "docker://" + other + "\n"},
		scriptRule{"get deployment", "app/demo:v0.9.0"},
	)
	st.Add(&store.Release{ID: "old", App: app.ID, Tag: "v0.5.0", Image: "app/demo:v0.5.0",
		ImageSummary: sum, Status: store.StatusSuccess})
	err := pipeline.ConfirmBeforeDeployForTest(r, true)
	if err == nil || !strings.Contains(err.Error(), "历史版本 v0.5.0") {
		t.Fatalf("与历史版本相同应返回重复确认: %v", err)
	}
	if dup := asDuplicate(err); dup == nil || dup.DuplicateTag != "v0.5.0" {
		t.Fatalf("重复信息不正确: %+v", dup)
	}
	// 重复来源写入记录
	got, _ := st.Get("r2")
	if got == nil || got.DuplicateOf != "old" {
		t.Fatalf("应记录重复来源: %+v", got)
	}

	// 4) 用户已针对同一摘要确认：放行并记录 TargetSummary
	r.SetKeepSameImageForTest(sum)
	if err := pipeline.ConfirmBeforeDeployForTest(r, true); err != nil {
		t.Fatalf("用户确认后应放行: %v", err)
	}
	if got, _ := st.Get("r2"); got == nil || got.TargetSummary != sum {
		t.Fatalf("应记录确认摘要: %+v", got)
	}

	// 5) 线上摘要与本次相同：返回 SameAsCurrent
	r3 := newRunner(&store.Release{ID: "r3", Tag: "v1.0.1", App: app.ID})
	r3.SetImageSummaryForTest(other)
	err = pipeline.ConfirmBeforeDeployForTest(r3, true)
	if dup := asDuplicate(err); dup == nil || !dup.SameAsCurrent {
		t.Fatalf("应与线上相同: %v", err)
	}
}

func TestPNormalizeImageID(t *testing.T) {
	sum := "sha256:" + strings.Repeat("b", 64)
	cases := map[string]string{
		"docker://" + sum + "\n": sum,
		"containerd://" + sum:    sum,
		"":                       "",
		"docker://sha256:short":  "",
	}
	for in, want := range cases {
		if got := pipeline.NormalizeImageIDForTest(in); got != want {
			t.Fatalf("normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func asDuplicate(err error) *pipeline.DuplicateDeployError {
	var d *pipeline.DuplicateDeployError
	if err != nil && errors.As(err, &d) {
		return d
	}
	return nil
}

// 确认后继续部署时，自动回滚依赖的"上一版本"必须是此刻线上的版本，而不是首次发布时的 PrevImage
func TestPResumeRefreshesPrevImage(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	sum := "sha256:" + strings.Repeat("9", 64)
	fake.addRules(
		scriptRule{"docker images -q", ""},
		scriptRule{"get deployment", "app/demo:v0.9.0"},
		scriptRule{"docker images --no-trunc", ""},
	)
	fake.prependRules(
		scriptRule{"xargs -r docker image inspect", sum + " []"},
		scriptRule{"docker image inspect", sum + " []"},
	)
	rel, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, rel.ID)
	if done.Status != store.StatusNeedsConfirm {
		t.Fatalf("应暂停等待确认: %s\n%s", done.Status, readLogFile(t, eng, rel.ID))
	}

	// 等待确认期间线上被切到 v0.9.5（首次发布时线上是 v0.9.0）
	fake.rules = nil
	fake.addRules(
		scriptRule{"docker images -q", ""},
		scriptRule{"get deployment", "app/demo:v0.9.5"},
		scriptRule{"docker images --no-trunc", ""},
	)
	fake.prependRules(
		scriptRule{"xargs -r docker image inspect", sum + " []"},
		scriptRule{"docker image inspect", sum + " []"},
	)
	if _, err := eng.ResumeRelease(rel.ID, sum); err != nil {
		t.Fatal(err)
	}
	final := waitDoneStrict(t, st, rel.ID)
	if final.Status != store.StatusSuccess {
		t.Fatalf("确认后应完成部署: %s\n%s", final.Error, readLogFile(t, eng, rel.ID))
	}
	if final.PrevImage != "app/demo:v0.9.5" {
		t.Fatalf("上一版本应刷新为确认时的线上版本，实际 %q", final.PrevImage)
	}
}

// 远端产物摘要与本地不一致时必须中止：防止共用目录时用错别人的产物构建
func TestPVerifyRemoteArtifact(t *testing.T) {
	eng, _, _, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	hub := pipeline.NewHub(filepath.Join(t.TempDir(), "v.log"))
	defer hub.Close()
	sum := strings.Repeat("a", 64)
	rel := &store.Release{ID: "v1", App: app.ID, Tag: "v1.0.0", FileSHA256: sum}
	r := pipeline.NewRunnerLike(eng, context.Background(), rel, app, eng.Cfg.SSHServers[0], hub, "", nil)
	var buf strings.Builder

	fake.addRules(scriptRule{"sha256sum", sum + "  /data/p.jar\n"})
	if err := pipeline.VerifyRemoteArtifactForTest(r, &buf, "/data/p.jar"); err != nil {
		t.Fatalf("摘要一致时应通过: %v", err)
	}
	if !strings.Contains(buf.String(), "校验通过") {
		t.Fatalf("应打印校验通过: %s", buf.String())
	}

	// 大小写不同也算一致（sha256sum 输出为小写，但不应因大小写误判）
	fake.rules = nil
	fake.addRules(scriptRule{"sha256sum", strings.ToUpper(sum) + "  /data/p.jar\n"})
	if err := pipeline.VerifyRemoteArtifactForTest(r, &strings.Builder{}, "/data/p.jar"); err != nil {
		t.Fatalf("大小写不同不应误判: %v", err)
	}

	// 内容不一致：中止
	fake.rules = nil
	fake.addRules(scriptRule{"sha256sum", strings.Repeat("b", 64) + "  /data/p.jar\n"})
	buf.Reset()
	if err := pipeline.VerifyRemoteArtifactForTest(r, &buf, "/data/p.jar"); err == nil || !strings.Contains(err.Error(), "不一致") {
		t.Fatalf("摘要不一致应中止: %v", err)
	}

	// 远端没有 sha256sum / 命令失败：只告警，不阻断
	fake.rules = nil
	fake.execErr = errors.New("command not found")
	if err := pipeline.VerifyRemoteArtifactForTest(r, &strings.Builder{}, "/data/p.jar"); err != nil {
		t.Fatalf("无法校验时应降级: %v", err)
	}
	fake.execErr = nil
	fake.addRules(scriptRule{"sha256sum", ""})
	if err := pipeline.VerifyRemoteArtifactForTest(r, &strings.Builder{}, "/data/p.jar"); err != nil {
		t.Fatalf("空输出应降级: %v", err)
	}
}

// cleanup 不再删除任何远程历史产物：只清掉本地上传缓存
func TestPCleanup(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	hub := pipeline.NewHub(filepath.Join(t.TempDir(), "c2.log"))
	defer hub.Close()
	for i, name := range []string{"a1.jar", "a2.jar", "a3.jar", "a4.jar", "a5.jar", "a6.jar"} {
		st.Add(&store.Release{ID: "r" + name, App: app.ID, Tag: "v1.0." + strconv.Itoa(i), FileName: name,
			Status: store.StatusSuccess, CreatedAt: time.Now().Add(time.Duration(i) * time.Minute)})
	}
	rel := &store.Release{ID: "cur", App: app.ID, Tag: "v2", FileName: "a6.jar"}
	r := pipeline.NewRunnerLike(eng, context.Background(), rel, app, eng.Cfg.SSHServers[0], hub, "", nil)
	pipeline.CleanupForTest(r)

	for _, c := range fake.snapshot() {
		if strings.Contains(c, "rm -f --") || strings.Contains(c, "docker image rm") || strings.Contains(c, "prune") {
			t.Fatalf("不应自动删除任何远程产物或镜像: %s", c)
		}
	}
}

// CurrentVersion 的降级路径：配置不完整 / 读到摘要而非镜像引用时返回空，而不是报错或给错版本
func TestPCurrentVersionDegrades(t *testing.T) {
	eng, _, _, fake, app := newScriptedEnv(t, config.DeployModeDocker)

	// Docker 配置不完整：不查询远程，直接返回空
	broken := *app
	broken.DockerContainer = ""
	v, err := eng.CurrentVersion(context.Background(), &broken)
	if err != nil || v.Tag != "" || v.Image != "" {
		t.Fatalf("配置不完整时应返回空: %+v %v", v, err)
	}

	// 容器里跑的是摘要（没有可读的 Tag）：不返回镜像名，避免页面展示 sha256
	fake.rules = nil
	fake.addRules(scriptRule{"docker inspect", "running sha256:" + strings.Repeat("a", 64)})
	v, err = eng.CurrentVersion(context.Background(), app)
	if err != nil || v.Tag != "" {
		t.Fatalf("摘要形式不应当作版本: %+v %v", v, err)
	}

	// 线上版本对应的发布记录：命中后应带出部署时间；不存在的 Tag 不应误配
	fake.rules = nil
	fake.addRules(scriptRule{"docker inspect", "running " + app.Image + ":v1.0.0"})
	eng.Store.Add(&store.Release{ID: "cur", App: app.ID, Tag: "v1.0.0", Status: store.StatusSuccess,
		CreatedAt: time.Now().Add(-time.Hour)})
	eng.Store.Add(&store.Release{ID: "other", App: app.ID, Tag: "v9.9.9", Status: store.StatusSuccess, CreatedAt: time.Now()})
	if v, _ := eng.CurrentVersion(context.Background(), app); v.DeployedAt == nil {
		t.Fatalf("应带出线上版本对应的部署时间: %+v", v)
	}
	fake.rules = nil
	fake.addRules(scriptRule{"docker inspect", "running " + app.Image + ":v0.1.0"})
	if v, _ := eng.CurrentVersion(context.Background(), app); v.DeployedAt != nil {
		t.Fatalf("没有对应发布记录时不应给部署时间: %+v", v)
	}

	// 服务器配置不存在：返回错误
	noSrv := *app
	noSrv.ServerID = "missing"
	if _, err := eng.CurrentVersion(context.Background(), &noSrv); err == nil {
		t.Fatal("服务器不存在应报错")
	}

	// K8s 形态读不到 Deployment（远端报错）：降级为空
	keng, _, _, kfake, kapp := newScriptedEnv(t, config.DeployModeK8s)
	kfake.execErr = errors.New("ssh 断开")
	v, err = keng.CurrentVersion(context.Background(), kapp)
	if err != nil || v.Tag != "" {
		t.Fatalf("远端失败应降级: %+v %v", v, err)
	}
}

// cleanup 只删除来自本地上传目录的 .part 缓存，其它本地产物不动
func TestPCleanupRemovesUploadCache(t *testing.T) {
	eng, _, _, _, app := newScriptedEnv(t, config.DeployModeK8s)
	hub := pipeline.NewHub(filepath.Join(t.TempDir(), "c3.log"))
	defer hub.Close()
	uploads := filepath.Join(eng.DataDir, "uploads")
	if err := os.MkdirAll(uploads, 0o755); err != nil {
		t.Fatal(err)
	}
	part := filepath.Join(uploads, "abc.part")
	if err := os.WriteFile(part, []byte("PK"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := pipeline.NewRunnerLike(eng, context.Background(), &store.Release{ID: "x", App: app.ID}, app, eng.Cfg.SSHServers[0], hub, part, nil)
	pipeline.CleanupForTest(r)
	if _, err := os.Stat(part); !os.IsNotExist(err) {
		t.Fatalf("上传缓存应被删除: %v", err)
	}

	// 不在上传目录里的本地产物不应被删
	other := filepath.Join(t.TempDir(), "keep.jar")
	if err := os.WriteFile(other, []byte("PK"), 0o644); err != nil {
		t.Fatal(err)
	}
	r = pipeline.NewRunnerLike(eng, context.Background(), &store.Release{ID: "y", App: app.ID}, app, eng.Cfg.SSHServers[0], hub, other, nil)
	pipeline.CleanupForTest(r)
	if _, err := os.Stat(other); err != nil {
		t.Fatalf("非上传缓存不应被删除: %v", err)
	}
}
