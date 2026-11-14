package pipeline

import (
	"context"
	"io"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/sshpool"
)

// 集中测试（tests/）使用的导出入口：包装包内步骤，生产代码不调用。

// ReplaceYamlImageForTest 包装 replaceYamlImage
func ReplaceYamlImageForTest(content, imageRepo, target string) (string, bool) {
	return replaceYamlImage(content, imageRepo, target)
}

// SplitImagePrefixForTest 包装 splitImagePrefix
func SplitImagePrefixForTest(line string) (prefix, value, suffix string, ok bool) {
	return splitImagePrefix(line)
}

// ValidateArtifactForTest 包装 validateArtifact
func ValidateArtifactForTest(w io.Writer, appType, path string) error {
	return validateArtifact(w, appType, path)
}

// FirstNForTest 包装 firstN
func FirstNForTest(s string, n int) string { return firstN(s, n) }

// IsYamlPathForTest 包装 isYamlPath
func IsYamlPathForTest(p string) bool { return isYamlPath(p) }

// ExtractTagForTest 包装 extractTag
func ExtractTagForTest(image string) string { return extractTag(image) }

// ImageRepoOfForTest 包装 imageRepoOf
func ImageRepoOfForTest(ref string) string { return imageRepoOf(ref) }

// K8sYamlFilePathForTest 包装 k8sYamlFilePath
func K8sYamlFilePathForTest(app *config.AppConfig) string { return k8sYamlFilePath(app) }

// ValidateK8sTargetForTest 包装 validateK8sTarget
func ValidateK8sTargetForTest(app *config.AppConfig) error { return validateK8sTarget(app) }

// ValidateAppForPipelineForTest 包装 validateAppForPipeline
func ValidateAppForPipelineForTest(app *config.AppConfig) error { return validateAppForPipeline(app) }

// RunLocalCommandForTest 包装 runLocalCommand
func RunLocalCommandForTest(ctx context.Context, workDir, cmd string, w io.Writer) error {
	return runLocalCommand(ctx, workDir, cmd, w)
}

// FileStatAndSHA256ForTest 包装 fileStatAndSHA256
func FileStatAndSHA256ForTest(path string) (string, int64, string, error) {
	return fileStatAndSHA256(path)
}

// CheckImageExistsForTest 包装 Engine.checkImageExists：回滚前确认目标镜像仍在服务器上
func CheckImageExistsForTest(ctx context.Context, e *Engine, srv *sshpool.ServerConfig, ref string) (bool, error) {
	return e.checkImageExists(ctx, srv, ref)
}

// MarkRunningForTest 占用应用的发布锁（模拟另一条流水线正在运行）
func MarkRunningForTest(e *Engine, appID, releaseID string, cancel context.CancelFunc) {
	e.mu.Lock()
	e.running[appID] = &activeRun{releaseID: releaseID, cancel: cancel}
	e.mu.Unlock()
}

// ReleaseRunningForTest 释放应用的发布锁
func ReleaseRunningForTest(e *Engine, appID string) { e.releaseRunning(appID) }
