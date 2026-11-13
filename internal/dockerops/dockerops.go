// Package dockerops 提供镜像维护相关的只读查询与显式删除。
// 本平台不在流水线里自动删除镜像；删除只能由用户在页面上明确勾选后触发。
package dockerops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/shellrun"
	"build-to-deploy/internal/sshpool"
)

// tagRe 与 config.TagRe 一致：tag 会拼进远程 shell，必须白名单
var tagRe = config.TagRe

// Image 服务器上某个应用镜像仓库下的一个 tag
type Image struct {
	Tag       string `json:"tag"`
	ID        string `json:"id"`
	Size      string `json:"size"`
	CreatedAt string `json:"createdAt"`
	// InUse 是否被容器（运行中或已停止）引用：in-use images must not delete
	InUse bool `json:"inUse"`
	// Protected 是否为当前应用的回滚保护版本（由调用方传入）
	Protected bool `json:"protected"`
	// ProtectedReason 保护原因，仅 Protected 时非空
	ProtectedReason string `json:"protectedReason,omitempty"`
}

// ListCmd 列出某镜像仓库下所有 tag 的详细信息。
// 使用 --format 而非 -f 以便逐行解析；%v / %s 字段可能为空（如 dangling），解析时兼容。
func ListCmd(imageRepo string) (string, error) {
	if !config.ImageRe.MatchString(imageRepo) {
		return "", fmt.Errorf("镜像名不合法: %q", imageRepo)
	}
	ref := sshpool.ShellQuote(imageRepo)
	return fmt.Sprintf(
		"docker images --no-trunc --format '%s' %s",
		`{{.Repository}}:{{.Tag}}|{{.ID}}|{{.Size}}|{{.CreatedAt}}`, ref), nil
}

// List 读取服务器上该镜像仓库下所有 tag。
func List(ctx context.Context, r shellrun.Runner, srv *sshpool.ServerConfig, imageRepo string) ([]Image, error) {
	if r == nil {
		return nil, errors.New("远程执行器未初始化")
	}
	cmd, err := ListCmd(imageRepo)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	out, err := r.ExecuteOutput(ctx, srv, cmd)
	if err != nil {
		return nil, fmt.Errorf("读取镜像列表失败: %w", err)
	}
	return ParseList(out, imageRepo), nil
}

// ParseList 把 docker images --format 的输出解析成 Image 列表，只保留指定仓库且 tag 合法的条目。
func ParseList(out, imageRepo string) []Image {
	var images []Image
	seen := map[string]bool{}
	prefix := imageRepo + ":"
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 4)
		if len(parts) < 4 {
			continue
		}
		ref := strings.TrimSpace(parts[0])
		tag, ok := strings.CutPrefix(ref, prefix)
		if !ok || !tagRe.MatchString(tag) || tag == "<none>" || seen[tag] {
			continue
		}
		seen[tag] = true
		images = append(images, Image{
			Tag:       tag,
			ID:        strings.TrimSpace(parts[1]),
			Size:      strings.TrimSpace(parts[2]),
			CreatedAt: strings.TrimSpace(parts[3]),
		})
	}
	return images
}

// UsedTags 返回该仓库下被容器引用（运行中或已停止）的 tag。
// docker ps -a --filter 按祖先镜像匹配：被引用镜像的 tag 也属于"祖先"，因此会一并返回。
func UsedTags(ctx context.Context, r shellrun.Runner, srv *sshpool.ServerConfig, imageRepo string) (map[string]bool, error) {
	cmd := fmt.Sprintf(
		"docker ps -a --no-trunc --filter %s --format '%s'",
		sshpool.ShellQuote("ancestor="+imageRepo), `{{.Image}}`)
	out, err := r.ExecuteOutput(ctx, srv, cmd)
	if err != nil {
		return nil, fmt.Errorf("查询容器引用的镜像失败: %w", err)
	}
	return parseUsedTags(out, imageRepo), nil
}

func parseUsedTags(out, imageRepo string) map[string]bool {
	used := map[string]bool{}
	prefix := imageRepo + ":"
	for _, line := range strings.Split(out, "\n") {
		ref := strings.TrimSpace(line)
		if tag, ok := strings.CutPrefix(ref, prefix); ok && tagRe.MatchString(tag) {
			used[tag] = true
		}
	}
	return used
}

// RemoveCmd 生成删除指定 tag 的命令。只接受本仓库下合法 tag 的引用，并在命令末尾回显确认删除的 ref，
// 调用方据此核对删除的确实是本应用镜像。
func RemoveCmd(imageRepo string, refs []string) (string, error) {
	if !config.ImageRe.MatchString(imageRepo) {
		return "", fmt.Errorf("镜像名不合法: %q", imageRepo)
	}
	if len(refs) == 0 {
		return "", errors.New("未指定要删除的镜像")
	}
	prefix := imageRepo + ":"
	quoted := make([]string, 0, len(refs))
	seen := map[string]bool{}
	for _, ref := range refs {
		tag, ok := strings.CutPrefix(ref, prefix)
		if !ok || !tagRe.MatchString(tag) {
			return "", fmt.Errorf("镜像引用 %q 不属于本应用镜像仓库 %s", ref, imageRepo)
		}
		if seen[ref] {
			continue
		}
		seen[ref] = true
		quoted = append(quoted, sshpool.ShellQuote(ref))
	}
	if len(quoted) == 0 {
		return "", errors.New("未指定要删除的镜像")
	}
	return "docker image rm " + strings.Join(quoted, " "), nil
}

// Remove 在服务器上删除指定镜像 tag，返回远程命令输出（供页面展示删除结果）。
func Remove(ctx context.Context, r shellrun.Runner, srv *sshpool.ServerConfig, imageRepo string, refs []string) (string, error) {
	cmd, err := RemoveCmd(imageRepo, refs)
	if err != nil {
		return "", err
	}
	if r == nil {
		return "", errors.New("远程执行器未初始化")
	}
	var buf strings.Builder
	err = r.RunCommand(ctx, srv, cmd, &buf)
	return buf.String(), err
}
