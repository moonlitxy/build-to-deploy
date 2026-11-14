package tests

import (
	"build-to-deploy/internal/pipeline"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/store"
)

func TestPMorePrecheckK8sConflictsAndWarnings(t *testing.T) {
	eng, _, _, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	fake.addRules(
		scriptRule{"docker images -q", "abcdef123456\n"},
		scriptRule{"get deployment", "app/demo:v1.0.0"},
		scriptRule{"docker image inspect", "sha256:" + strings.Repeat("a", 64) + " []"},
	)
	res, err := eng.Precheck(context.Background(), app, "v1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Conflicts) < 2 || res.Current != "app/demo:v1.0.0" || res.CurrentTag != "v1.0.0" {
		t.Fatalf("预检结果 = %+v", res)
	}

	// 执行失败：只告警，不阻断
	fake.rules = nil
	fake.execErr = errors.New("ssh 断了")
	res, err = eng.Precheck(context.Background(), app, "v2.0.0")
	if err != nil || len(res.Warnings) == 0 {
		t.Fatalf("远程失败应只告警: %+v %v", res, err)
	}
}

func TestPMorePrecheckDockerRuntime(t *testing.T) {
	eng, _, _, fake, app := newScriptedEnv(t, config.DeployModeDocker)
	fake.addRules(
		scriptRule{"docker images -q", ""},
		scriptRule{"docker inspect -f", "running app/demo:v2.0.0\n"},
		scriptRule{"docker image inspect -f", "sha256:" + strings.Repeat("b", 64) + " []"},
	)
	res, err := eng.Precheck(context.Background(), app, "v2.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if res.Current != "app/demo:v2.0.0" || len(res.Conflicts) == 0 || !strings.HasPrefix(res.CurrentImage, "sha256:") {
		t.Fatalf("容器预检 = %+v", res)
	}
	// 容器不存在：无状态
	fake.rules = []scriptRule{{contains: "docker images -q", out: ""}}
	if res, err := eng.Precheck(context.Background(), app, "v3.0.0"); err != nil || res.Current != "" {
		t.Fatalf("容器不存在时预检 = %+v %v", res, err)
	}
	// 配置不完整：告警
	bad := *app
	bad.DockerContainer = "bad name"
	res, err = eng.Precheck(context.Background(), &bad, "v3.0.0")
	if err != nil || len(res.Warnings) == 0 {
		t.Fatalf("非法容器名应告警: %+v %v", res, err)
	}
	// 查询失败：告警
	fake.execErr = errors.New("fail")
	res, _ = eng.Precheck(context.Background(), app, "v3.1.0")
	if len(res.Warnings) == 0 {
		t.Fatalf("查询失败应告警: %+v", res)
	}
}

func TestPMoreInspectArtifactVariants(t *testing.T) {
	eng, _, _, fake, app := newScriptedEnv(t, config.DeployModeDocker)
	_ = fake
	// 本地产物：能算出指纹；产物不存在必须报错，不跳过
	fp, err := eng.InspectArtifact(app, pipeline.ArtifactSource{Tag: "v1", LocalArtifact: app.ResolveLocalArtifactPath()})
	if err != nil || fp.Fingerprint == "" || fp.SHA256 == "" {
		t.Fatalf("本地产物指纹 = %+v %v", fp, err)
	}
	if _, err := eng.InspectArtifact(app, pipeline.ArtifactSource{Tag: "v1", LocalArtifact: "/nonexistent/x.jar"}); err == nil {
		t.Fatal("缺失产物应报错")
	}
	// 上传任务
	up, _ := eng.Uploads.Create("a.jar", 4, "t")
	eng.Uploads.WriteChunk(up.ID, 0, strings.NewReader("1234"))
	if _, err := eng.InspectArtifact(app, pipeline.ArtifactSource{Tag: "v1", UploadID: up.ID}); err == nil {
		t.Fatal("未完成上传应报错")
	}
	eng.Uploads.Complete(up.ID)
	done, _ := eng.Uploads.Get(up.ID)
	snap := done.Snapshot()
	if fp, err := eng.InspectArtifact(app, pipeline.ArtifactSource{Tag: "v1", Completed: &snap}); err != nil || fp.Fingerprint == "" {
		t.Fatalf("已完成上传指纹 = %+v %v", fp, err)
	}
	if _, err := eng.InspectArtifact(app, pipeline.ArtifactSource{Tag: "v1", UploadID: up.ID}); err != nil {
		t.Fatalf("按上传 ID 计算指纹: %v", err)
	}
	// 构建命令无法解析
	app2 := *app
	app2.BuildCmd = "echo build"
	if fp, err := eng.InspectArtifact(&app2, pipeline.ArtifactSource{Tag: "v1", LocalArtifact: app.ResolveLocalArtifactPath()}); err != nil || fp.Fingerprint != "" {
		t.Fatalf("无法解析构建命令 = %+v %v", fp, err)
	}
	// 跳过原因
	if r := eng.FingerprintSkipReason(app, pipeline.ArtifactSource{}); r == "" {
		t.Fatal("应给出跳过原因")
	}
	if r := eng.FingerprintSkipReason(&app2, pipeline.ArtifactSource{Tag: "v1"}); !strings.Contains(r, "无法静态解析") {
		t.Fatalf("原因 = %s", r)
	}
	if r := eng.FingerprintSkipReason(app, pipeline.ArtifactSource{LocalArtifact: "/nope.jar"}); !strings.Contains(r, "找不到") {
		t.Fatalf("原因 = %s", r)
	}
	if r := eng.FingerprintSkipReason(app, pipeline.ArtifactSource{LocalArtifact: app.ResolveLocalArtifactPath()}); r == "" {
		t.Fatal("应有默认原因")
	}
}

func TestPMoreReuseImageOnRebuild(t *testing.T) {
	eng, cfg, st, fake, app := newScriptedEnv(t, config.DeployModeDocker)
	sum := "sha256:" + strings.Repeat("c", 64)
	// 先做一次成功发布，写入镜像摘要与指纹
	fake.addRules(
		scriptRule{"docker images -q", ""},
		scriptRule{"docker image inspect -f", sum + " []"},
	)
	first, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, first.ID)
	if done.Status != store.StatusSuccess || done.ImageSummary != sum {
		t.Fatalf("首次发布 = %+v (%s)", done, readLogFile(t, eng, first.ID))
	}
	if !strings.Contains(readLogFile(t, eng, first.ID), "记录构建输入指纹标签") {
		t.Fatalf("应写入指纹标签:\n%s", readLogFile(t, eng, first.ID))
	}

	// 同产物再发布：应复用已有镜像（只追加 Tag），而不是重新 docker build
	fake.cmds = nil
	second, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.1", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	done2 := waitDoneStrict(t, st, second.ID)
	log := readLogFile(t, eng, second.ID)
	if done2.Status != store.StatusSuccess {
		t.Fatalf("复用发布失败: %s\n%s", done2.Error, log)
	}
	cmds := strings.Join(fake.snapshot(), "\n")
	if !strings.Contains(cmds, "docker tag") || strings.Contains(cmds, "docker build -t") {
		t.Fatalf("应复用镜像（docker tag）而不重新构建:\n%s\n--- 日志 ---\n%s", cmds, log)
	}
	if !strings.Contains(log, "复用构建结果") {
		t.Fatalf("日志应说明复用:\n%s", log)
	}
	_ = cfg
}

// Docker 形态：线上容器运行的镜像与本次重建结果内容一致时，必须提醒并拒绝部署（换 Tag 也不会改变线上）
func TestPMoreWarnWhenRebuildSameAsDockerContainer(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeDocker)
	sum := "sha256:" + strings.Repeat("9", 64)
	fake.addRules(
		scriptRule{"docker images -q", ""},
		scriptRule{"docker inspect", "running " + app.Image + ":v0.9.0"},
		scriptRule{"docker images --no-trunc", ""},
	)
	fake.prependRules(scriptRule{"docker image inspect", sum + " []"})
	rel, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true, NoReuse: true})
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, rel.ID)
	log := readLogFile(t, eng, rel.ID)
	if !strings.Contains(log, "与线上当前运行的版本 v0.9.0 内容完全相同") {
		t.Fatalf("重建结果与线上容器一致时应提醒:\n%s", log)
	}
	if done.Status != store.StatusNeedsConfirm {
		t.Fatalf("镜像摘要与线上容器一致时应暂停并等待确认: status=%s\n%s", done.Status, log)
	}
	if strings.Contains(log, "docker rm -f") {
		t.Fatalf("被拦截时不应重启容器:\n%s", log)
	}
}

// 直接执行 docker build 时，若结果与线上正在运行的版本内容一致，即使换成别的 Tag 也要提醒
func TestPMoreWarnWhenBuiltImageSameAsCurrent(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	// 线上运行的 v0.9.0 与本次构建结果的镜像摘要一致（两条 inspects 都返回同一个摘要）
	sum := "sha256:" + strings.Repeat("9", 64)
	// 线上镜像的内容摘要查询（含 xargs 的更具体命令）必须排在 "docker image inspect" 之前命中
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
	log := readLogFile(t, eng, rel.ID)
	// 判定依据是镜像摘要：构建阶段先提醒，部署前的一致性确认阶段再拦截
	if !strings.Contains(log, "与线上当前运行的版本 v0.9.0 内容完全相同") {
		t.Fatalf("镜像内容与线上相同时应提醒，换 Tag 也一样:\n%s", log)
	}
	if done.Status != store.StatusNeedsConfirm || !strings.Contains(done.Error, "部署后线上不会有任何变化") {
		t.Fatalf("镜像摘要与线上相同时应暂停并等待确认: status=%s err=%s\n%s", done.Status, done.Error, log)
	}
	if strings.Contains(log, "kubectl apply") || strings.Contains(log, "rollout status") {
		t.Fatalf("被拦截时不应执行部署:\n%s", log)
	}
}

// 镜像内容与线上相同 → 暂停；用户确认同一摘要后从部署阶段继续，不重新构建
func TestPMoreResumeAfterDuplicateConfirm(t *testing.T) {
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
		t.Fatalf("应进入待确认状态: %s\n%s", done.Status, readLogFile(t, eng, rel.ID))
	}
	if strings.Contains(readLogFile(t, eng, rel.ID), "kubectl apply") {
		t.Fatal("待确认时不应部署")
	}

	// 摘要对不上：拒绝继续
	if _, err := eng.ResumeRelease(rel.ID, "sha256:other"); err == nil {
		t.Fatal("摘要不一致时应拒绝继续")
	}
	// 状态已不是待确认时也拒绝
	if _, err := eng.ResumeRelease("nope", sum); err == nil {
		t.Fatal("不存在的记录应报错")
	}

	fake.cmds = nil
	if _, err := eng.ResumeRelease(rel.ID, sum); err != nil {
		t.Fatal(err)
	}
	final := waitDoneStrict(t, st, rel.ID)
	log := readLogFile(t, eng, rel.ID)
	if final.Status != store.StatusSuccess {
		t.Fatalf("确认后应继续完成部署: %s\n%s", final.Status, log)
	}
	if final.TargetSummary != sum {
		t.Fatalf("应记录确认的镜像摘要: %q", final.TargetSummary)
	}
	cmds := strings.Join(fake.snapshot(), "\n")
	if !strings.Contains(cmds, "kubectl apply") || strings.Contains(cmds, "docker build -t") || strings.Contains(cmds, "docker tag") {
		t.Fatalf("继续时只应部署，不应重新构建或重新打 Tag:\n%s", cmds)
	}
	// 再次继续：状态已不是待确认
	if _, err := eng.ResumeRelease(rel.ID, sum); err == nil {
		t.Fatal("已完成的记录不应再次继续")
	}
}

// 并发保护：同一应用已有流水线在跑时，确认继续部署必须被拒绝，不能出现两条流水线同时改线上
func TestPMoreResumeRejectsWhenAnotherReleaseRunning(t *testing.T) {
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
	waitDoneStrict(t, st, rel.ID)

	// 模拟另一条发布正在占用该应用
	var cancellation context.CancelFunc = func() {}
	done := make(chan struct{})
	pipeline.MarkRunningForTest(eng, app.ID, "other-release", cancellation)
	defer func() {
		pipeline.ReleaseRunningForTest(eng, app.ID)
		close(done)
	}()
	if _, err := eng.ResumeRelease(rel.ID, sum); err == nil || !strings.Contains(err.Error(), "正在发布中") {
		t.Fatalf("并发时应拒绝继续: %v", err)
	}
}

// TestPMoreNoReuseForcesRebuild 用户明确选择"仍然重新构建"时不复用内容相同的已有镜像
func TestPMoreNoReuseForcesRebuild(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeDocker)
	sum := "sha256:" + strings.Repeat("d", 64)
	fake.addRules(
		scriptRule{"docker images -q", ""},
		scriptRule{"docker image inspect -f", sum + " []"},
	)
	first, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v2.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	waitDoneStrict(t, st, first.ID)

	fake.cmds = nil
	second, err := eng.StartDeploy(pipeline.StartRequest{
		AppID:            app.ID,
		Tag:              "v2.0.1",
		UseLocalArtifact: true,
		Force:            true,
		NoReuse:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, second.ID)
	log := readLogFile(t, eng, second.ID)
	if done.Status != store.StatusSuccess {
		t.Fatalf("重新构建发布失败: %s\n%s", done.Error, log)
	}
	if !strings.Contains(log, "已选择重新构建") {
		t.Fatalf("日志应说明用户选择了重新构建:\n%s", log)
	}
	if strings.Contains(log, "复用构建结果") {
		t.Fatalf("选择重新构建时不应复用镜像:\n%s", log)
	}
	cmds := strings.Join(fake.snapshot(), "\n")
	if !strings.Contains(cmds, "docker build -t") {
		t.Fatalf("应执行 docker build:\n%s", cmds)
	}
}

// 回滚前的镜像存在性检查：远程命令失败或引用非法时都要明确报错，而不是放行
func TestPMoreRollbackImageCheckErrors(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	first, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	waitDoneStrict(t, st, first.ID)
	deadline := time.Now().Add(3 * time.Second)
	for pipeline.IsRunning(eng, app.ID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	// 远程查询失败
	fake.execErr = errors.New("ssh 断开")
	if _, err := eng.StartRollback(pipeline.RollbackRequest{ReleaseID: first.ID}); err == nil || !strings.Contains(err.Error(), "是否存在失败") {
		t.Fatalf("远程失败应报错: %v", err)
	}
	// 调用方先校验过引用，这里再覆盖一次函数级白名单
	fake.execErr = nil
	exists, err := pipeline.CheckImageExistsForTest(context.Background(), eng, eng.Cfg.SSHServers[0], "bad ref;rm -rf")
	if err == nil || exists {
		t.Fatalf("非法引用应报错: exists=%v err=%v", exists, err)
	}
}

func TestPMoreRollbackErrorsAndTargets(t *testing.T) {
	eng, _, st, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	first, err := eng.StartDeploy(pipeline.StartRequest{AppID: app.ID, Tag: "v1.0.0", UseLocalArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	waitDoneStrict(t, st, first.ID)
	if _, err := eng.StartRollback(pipeline.RollbackRequest{ReleaseID: "missing"}); err == nil {
		t.Fatal("不存在的历史记录应报错")
	}
	// 回滚前会确认目标镜像仍在服务器上：先模拟"镜像已被清理"，应拒绝回滚且不建记录
	fake.addRules(scriptRule{"docker image inspect -f", "1"})
	if _, err := eng.StartRollback(pipeline.RollbackRequest{ReleaseID: first.ID}); err == nil {
		t.Fatal("目标镜像不存在时应拒绝回滚")
	} else if !strings.Contains(err.Error(), "已不在服务器") || !errors.Is(err, pipeline.ErrImageMissing) {
		// 必须能被 errors.Is 识别为"镜像不存在"，API 层据此返回 409 而不是 400
		t.Fatalf("错误信息应说明镜像不存在: %v", err)
	}
	if rels := st.List(app.ID, 0); len(rels) != 1 {
		t.Fatalf("拒绝回滚时不应创建发布记录: %d 条", len(rels))
	}
	// 镜像恢复后可以正常回滚（scriptedRunner 按顺序命中第一条匹配规则）
	fake.rules = nil
	fake.addRules(scriptRule{"docker image inspect -f", "0"})
	rb, err := eng.StartRollback(pipeline.RollbackRequest{ReleaseID: first.ID})
	if err != nil {
		t.Fatal(err)
	}
	done := waitDoneStrict(t, st, rb.ID)
	if done.Kind != "rollback" || done.RollbackOf != first.ID {
		t.Fatalf("回滚记录 = %+v", done)
	}
	log := readLogFile(t, eng, rb.ID)
	if !strings.Contains(log, "开始回滚") || !strings.Contains(log, "已确认目标镜像仍存在") {
		t.Fatalf("回滚日志缺失:\n%s", log)
	}
}

func TestPMoreTryAutoRollbackK8sBranches(t *testing.T) {
	eng, _, _, fake, app := newScriptedEnv(t, config.DeployModeK8s)
	hub := pipeline.NewHub(filepath.Join(t.TempDir(), "rb.log"))
	defer hub.Close()
	rel := &store.Release{ID: "r", Image: "app/demo:v2", PrevImage: "app/demo:v1"}
	r := pipeline.NewRunnerLike(eng, context.Background(), rel, app, eng.Cfg.SSHServers[0], hub, "", nil)

	// 关闭自动回滚：什么都不做
	app.AutoRollback = false
	pipeline.TryAutoRollbackForTest(r, errors.New("x"))
	app.AutoRollback = true

	// 上一版本与当前相同
	rel.PrevImage = rel.Image
	pipeline.TryAutoRollbackForTest(r, errors.New("x"))
	rel.PrevImage = "bad image;rm -rf"
	pipeline.TryAutoRollbackForTest(r, errors.New("x"))
	rel.PrevImage = "app/demo:v1"

	// 用户取消
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.SetContext(ctx)
	pipeline.TryAutoRollbackForTest(r, context.Canceled)
	r.SetContext(context.Background())

	// 正常回滚：回写 YAML 并 apply
	if err := pipeline.UpdateRemoteYamlForTest(r, hub, rel.PrevImage); err != nil {
		t.Fatal(err)
	}
	pipeline.TryAutoRollbackForTest(r, errors.New("部署失败"))
	b, _ := os.ReadFile(hub.LogPathForTest())
	if !strings.Contains(string(b), "已自动回滚") {
		t.Fatalf("回滚日志 = %s", string(b))
	}
	_ = fake

	// 回滚失败：读取 YAML 失败
	delete(fake.files, app.K8sYamlDir+"/demo.yaml")
	r.SetHub(pipeline.NewHub(filepath.Join(t.TempDir(), "rb2.log")))
	defer r.CloseHub()
	pipeline.TryAutoRollbackForTest(r, errors.New("部署失败"))
	b, _ = os.ReadFile(r.Hub().LogPathForTest())
	if !strings.Contains(string(b), "自动回滚未能完成") {
		t.Fatalf("回滚失败日志 = %s", string(b))
	}
}

func TestPMoreCancelAndHub(t *testing.T) {
	eng, _, _, _, app := newScriptedEnv(t, config.DeployModeK8s)
	pipeline.MarkRunning(eng, app.ID)
	called := false
	pipeline.MarkRunningCancel(eng, app.ID, func() { called = true })
	if err := eng.Cancel("test-rel1"); err != nil || !called {
		t.Fatalf("Cancel = %v, called=%v", err, called)
	}
	if !pipeline.IsRunning(eng, app.ID) {
		t.Fatal("取消只触发 cancel，运行表由流水线 goroutine 结束时清理")
	}
	pipeline.ClearRunning(eng, app.ID)
	if err := eng.Cancel("nope"); err == nil {
		t.Fatal("取消不存在任务应报错")
	}

	hub := pipeline.NewHub(filepath.Join(t.TempDir(), "h.log"))
	history, ch := hub.Subscribe()
	if len(history) != 0 {
		t.Fatal("新订阅者历史为空")
	}
	hub.Logf("line-1")
	select {
	case e := <-ch:
		if data, _ := e.Data.(string); !strings.Contains(data, "line-1") {
			t.Fatalf("事件 = %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("订阅者应收到日志")
	}
	hub.Unsubscribe(ch)
	hub.Logf("line-2")
	history, ch2 := hub.Subscribe()
	if len(history) == 0 || !strings.Contains(strings.Join(history, "\n"), "line-2") {
		t.Fatalf("新订阅者应回放历史: %v", history)
	}
	hub.Unsubscribe(ch2)
	hub.Close()
}

func TestPMoreStartDeployValidationErrors(t *testing.T) {
	eng, cfg, _, _, app := newScriptedEnv(t, config.DeployModeDocker)
	ctx := context.Background()
	if _, err := eng.StartDeployContext(ctx, pipeline.StartRequest{AppID: "nope"}); err == nil {
		t.Fatal("未知应用应报错")
	}
	app.ServerID = "missing"
	if _, err := eng.StartDeployContext(ctx, pipeline.StartRequest{AppID: app.ID, Tag: "v1"}); err == nil {
		t.Fatal("未知服务器应报错")
	}
	app.ServerID = "s1"
	if _, err := eng.StartDeployContext(ctx, pipeline.StartRequest{AppID: app.ID, Tag: "bad tag"}); err == nil {
		t.Fatal("非法 Tag 应报错")
	}
	if _, err := eng.StartDeployContext(ctx, pipeline.StartRequest{AppID: app.ID, Tag: "v1"}); err == nil {
		t.Fatal("没有任何产物来源应报错")
	}
	// 上传任务不完整
	up, _ := eng.Uploads.Create("a.jar", 10, "t")
	if _, err := eng.StartDeployContext(ctx, pipeline.StartRequest{AppID: app.ID, Tag: "v1", UploadID: up.ID}); err == nil {
		t.Fatal("未完成上传应报错")
	}
	// 本地编译配置缺失
	app.LocalBuildCmd = ""
	app.LocalArtifactPath = ""
	cfg.Save()
	if _, err := eng.StartDeployContext(ctx, pipeline.StartRequest{AppID: app.ID, Tag: "v1", RunLocalBuild: true}); err == nil {
		t.Fatal("缺少本地编译配置应报错")
	}
	var b bytes.Buffer
	if _, err := eng.InspectArtifact(app, pipeline.ArtifactSource{}); err != nil {
		t.Fatalf("无产物来源不应报错: %v", err)
	}
	_ = b
}
