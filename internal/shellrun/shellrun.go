// Package shellrun 定义远程命令执行的最小接口，让流水线/接口层可以注入假实现做测试。
package shellrun

import (
	"context"
	"io"

	"build-to-deploy/internal/sshpool"
)

// Runner 远程命令与文件的执行接口：生产实现为 sshpool.Pool
type Runner interface {
	RunCommand(ctx context.Context, srv *sshpool.ServerConfig, cmd string, w io.Writer) error
	ExecuteOutput(ctx context.Context, srv *sshpool.ServerConfig, cmd string) (string, error)
	ReadRemoteFile(srv *sshpool.ServerConfig, path string) (string, error)
	WriteRemoteFile(srv *sshpool.ServerConfig, path, content string) error
	UploadFile(ctx context.Context, srv *sshpool.ServerConfig, localPath, remotePath string, progressFn func(uploaded, total int64)) error
}
