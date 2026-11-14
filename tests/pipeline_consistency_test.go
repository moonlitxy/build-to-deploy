package tests

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

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
