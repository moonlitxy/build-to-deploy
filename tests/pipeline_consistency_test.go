package tests

import (
	"context"
	"errors"
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

// cleanup 只删除当前产物目录下真实存在、且属于本应用上传的文件
func TestPCleanup(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	hub := pipeline.NewHub(filepath.Join(t.TempDir(), "c2.log"))
	defer hub.Close()
	for i, name := range []string{"a1.jar", "a2.jar", "a3.jar", "a4.jar", "a5.jar", "a6.jar"} {
		st.Add(&store.Release{ID: "r" + name, App: app.ID, Tag: "v1.0." + strconv.Itoa(i), FileName: name,
			Status: store.StatusSuccess, CreatedAt: time.Now().Add(time.Duration(i) * time.Minute)})
	}
	// 非法文件名不参与清理
	st.Add(&store.Release{ID: "bad", App: app.ID, Tag: "v9", FileName: "../evil", Status: store.StatusSuccess})
	rel := &store.Release{ID: "cur", App: app.ID, Tag: "v2", FileName: "a6.jar"}
	r := pipeline.NewRunnerLike(eng, context.Background(), rel, app, eng.Cfg.SSHServers[0], hub, "", nil)
	pipeline.CleanupForTest(r)

	var cleanCmd string
	for _, c := range fake.snapshot() {
		if strings.Contains(c, "rm -f --") {
			cleanCmd = c
		}
	}
	if cleanCmd == "" {
		t.Fatal("超过 5 个版本时应触发清理")
	}
	// 文件名全部先列出、再在远端按"真实存在"过滤后 tail，因此最新文件出现在列表里是正常的；
	// 关键是要保留最近 5 个（tail -n +6）、非法文件名不得出现
	if strings.Contains(cleanCmd, "../evil") {
		t.Fatalf("非法文件名不应进入清理列表: %s", cleanCmd)
	}
	if !strings.Contains(cleanCmd, "tail -n +6") {
		t.Fatalf("应保留最近 5 个版本: %s", cleanCmd)
	}
	if !strings.Contains(cleanCmd, "[ -f") {
		t.Fatalf("清理前应确认远端文件真实存在: %s", cleanCmd)
	}
}
