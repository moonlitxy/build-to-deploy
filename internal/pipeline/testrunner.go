package pipeline

import (
	"context"
	"io"

	"build-to-deploy/internal/buildstage"
	"build-to-deploy/internal/config"
	"build-to-deploy/internal/sshpool"
	"build-to-deploy/internal/store"
)

// 测试辅助类型：集中放在 tests/ 下的用例需要构造单个流水线运行器来验证某一步骤，
// 生产代码不使用这些构造入口，行为不变。

// RunnerLike 包装流水线运行器
type RunnerLike struct {
	pipelineRunner
}

// NewRunnerLike 按给定参数构造独立运行器（不启动流水线）
func NewRunnerLike(engine *Engine, ctx context.Context, rel *store.Release, app *config.AppConfig,
	srv *sshpool.ServerConfig, hub *Hub, artifact string, precheck *PrecheckResult) *RunnerLike {
	return &RunnerLike{pipelineRunner{
		engine: engine, ctx: ctx, rel: rel, app: app, srv: srv, hub: hub, artifact: artifact, precheck: precheck,
	}}
}

// NewHub 按日志路径创建日志通道
func NewHub(logPath string) *Hub { return newHub(logPath) }

// Hub 返回运行器当前使用的日志通道
func (r *RunnerLike) Hub() *Hub { return r.hub }

// SetHub 替换运行器日志通道
func (r *RunnerLike) SetHub(h *Hub) { r.hub = h }

// CloseHub 关闭日志通道
func (r *RunnerLike) CloseHub() { r.hub.Close() }

// AppConfigAlias 便于 tests 目录引用配置类型
type AppConfigAlias = config.AppConfig

// SpecAlias 便于 tests 目录引用构建要素类型
type SpecAlias = buildstage.Spec

// MarkRunning 把某个应用标记为"正在发布"，用于验证并发锁（测试用）
func MarkRunning(engine *Engine, appID string) func() {
	engine.mu.Lock()
	engine.running[appID] = &activeRun{releaseID: "test-busy", cancel: func() {}}
	engine.mu.Unlock()
	return func() {
		engine.mu.Lock()
		delete(engine.running, appID)
		engine.mu.Unlock()
	}
}

// IsRunning 应用当前是否有发布在跑（测试用）
func IsRunning(engine *Engine, appID string) bool {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	_, ok := engine.running[appID]
	return ok
}

// MarkRunningCancel 标记应用正在发布，并指定"取消"时执行的回调（测试用）
func MarkRunningCancel(engine *Engine, appID string, cancel func()) func() {
	engine.mu.Lock()
	engine.running[appID] = &activeRun{releaseID: "test-rel1", cancel: cancel}
	engine.mu.Unlock()
	return func() { ClearRunning(engine, appID) }
}

// ClearRunning 清除发布标记（测试用）
func ClearRunning(engine *Engine, appID string) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	delete(engine.running, appID)
	return nil
}

// SetContext 替换运行器上下文（测试模拟取消）
func (r *RunnerLike) SetContext(ctx context.Context) { r.ctx = ctx }

// LogPathForTest 返回日志通道写入的文件路径
func (h *Hub) LogPathForTest() string { return h.logPath }

// DeployDockerForTest 包装 deployDocker（容器形态部署单步）
func DeployDockerForTest(r *RunnerLike, w io.Writer, imageRef string) error {
	return r.deployDocker(w, imageRef)
}

// TryAutoRollbackForTest 包装 tryAutoRollback
func TryAutoRollbackForTest(r *RunnerLike, cause error) {
	r.tryAutoRollback(cause, "测试")
}

// UpdateRemoteYamlForTest 包装 updateRemoteK8sYamlImage
func UpdateRemoteYamlForTest(r *RunnerLike, w io.Writer, image string) error {
	return r.updateRemoteK8sYamlImage(w, image)
}

// SetKeepSameImageForTest 设置"用户已确认的镜像摘要"
func (r *RunnerLike) SetKeepSameImageForTest(sum string) { r.keepSameImage = sum }

// SetPrecheckForTest 设置预检结果（线上版本信息）
func (r *RunnerLike) SetPrecheckForTest(pre *PrecheckResult) { r.precheck = pre }

// SetImageSummaryForTest 设置本次镜像摘要
func (r *RunnerLike) SetImageSummaryForTest(sum string) {
	r.rel.ImageSummary = sum
}

// ConfirmBeforeDeployForTest 包装 confirmBeforeDeploy
func ConfirmBeforeDeployForTest(r *RunnerLike, built bool) error { return r.confirmBeforeDeploy(built) }

// NormalizeImageIDForTest 包装 normalizeImageID
func NormalizeImageIDForTest(s string) string { return normalizeImageID(s) }
