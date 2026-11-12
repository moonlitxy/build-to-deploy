package api

import (
	"build-to-deploy/internal/config"
	"build-to-deploy/internal/pipeline"
)

// MaskedValueForTest 暴露脱敏占位符，供集中测试断言"回传掩码不覆盖密码"
const MaskedValueForTest = maskedValue

// ServerForTest 是测试辅助类型，让集中放在 tests/ 下的用例访问接口层内部对象。
// 生产代码不使用。
type ServerForTest struct{ srv *Server }

// NewServerForTest 包装 NewServer，返回带辅助方法的测试句柄
func NewServerForTest(srv *Server) *ServerForTest { return &ServerForTest{srv: srv} }

// Engine 暴露流水线引擎（测试中替换执行器、查询发布记录）
func (t *ServerForTest) Engine() *pipeline.Engine { return t.srv.engine }

// Config 暴露配置（测试中修改服务器与应用配置）
func (t *ServerForTest) Config() *config.Config { return t.srv.cfg }

// HasPool 是否接入了真实 SSH 连接池（nil 时连接测试接口无法拨号）
func (t *ServerForTest) HasPool() bool { return t.srv.pool != nil }

// SanitizePrompt 暴露对话框标题清洗（AppleScript/zenity 参数注入防护）
func SanitizePrompt(p string) string { return sanitizePrompt(p) }

// ReadLines 暴露日志环形缓冲读取
func ReadLines(path string) []string { return readLines(path) }

// MaxReadLines 日志环形缓冲窗口大小
const MaxReadLines = maxReadLines
