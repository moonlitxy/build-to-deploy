package sshpool

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// 集中测试（tests/）使用的导出入口与连接池注入点：生产代码按默认值运行，行为不变。

// remoteFile 是 SFTP 单文件的读写能力子集，便于用本地文件做传输层测试
type remoteFile interface {
	io.Reader
	io.Writer
	io.Closer
}

// FS 传输层依赖的 SFTP 能力子集（*sftp.Client 满足；测试可注入本地实现）
type FS interface {
	MkdirAll(path string) error
	Create(path string) (remoteFile, error)
	Open(path string) (remoteFile, error)
	Stat(path string) (os.FileInfo, error)
	Chmod(path string, mode os.FileMode) error
	Remove(path string) error
	Rename(oldpath, newpath string) error
	PosixRename(oldpath, newpath string) error
}

// RemoteFile 暴露单文件读写能力，供集中测试实现本地文件系统
type RemoteFile = remoteFile

// sftpFSAdapter 把 *sftp.Client 适配为 FS（Create/Open 返回类型协变由适配器处理）
type sftpFSAdapter struct{ c *sftp.Client }

func (a sftpFSAdapter) MkdirAll(p string) error             { return a.c.MkdirAll(p) }
func (a sftpFSAdapter) Stat(p string) (os.FileInfo, error)  { return a.c.Stat(p) }
func (a sftpFSAdapter) Chmod(p string, m os.FileMode) error { return a.c.Chmod(p, m) }
func (a sftpFSAdapter) Remove(p string) error               { return a.c.Remove(p) }
func (a sftpFSAdapter) Rename(o, n string) error            { return a.c.Rename(o, n) }
func (a sftpFSAdapter) PosixRename(o, n string) error       { return a.c.PosixRename(o, n) }
func (a sftpFSAdapter) Create(p string) (remoteFile, error) {
	f, err := a.c.Create(p)
	if err != nil {
		return nil, err
	}
	return f, nil
}
func (a sftpFSAdapter) Open(p string) (remoteFile, error) {
	f, err := a.c.Open(p)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// DialForTest 包装 dialSSH
func DialForTest(cfg *ServerConfig) (*ssh.Client, error) { return dialSSH(cfg) }

// SendKeepaliveForTest 包装 sendKeepalive
func SendKeepaliveForTest(cli *ssh.Client) error { return sendKeepalive(cli) }

// UploadSFTPForTest 暴露传输层上传（接受测试注入的文件系统）
func UploadSFTPForTest(ctx context.Context, fs FS, local, remote string, fn func(int64, int64)) error {
	return UploadSFTP(ctx, fs, local, remote, fn)
}

// ReadSFTPForTest 暴露传输层读取
func ReadSFTPForTest(fs FS, remote string) (string, error) { return ReadSFTP(fs, remote) }

// WriteSFTPForTest 暴露传输层写入
func WriteSFTPForTest(fs FS, remote, content string) error { return WriteSFTP(fs, remote, content) }

// UploadFileForTest 走池上传（验证取连接与错误透传）
func UploadFileForTest(p *Pool, cfg *ServerConfig, local, remote string, fn func(int64, int64)) error {
	return p.UploadFile(context.Background(), cfg, local, remote, fn)
}

// ReadRemoteFileForTest 走池读取
func ReadRemoteFileForTest(p *Pool, cfg *ServerConfig, remote string) (string, error) {
	return p.ReadRemoteFile(cfg, remote)
}

// WriteRemoteFileForTest 走池写入
func WriteRemoteFileForTest(p *Pool, cfg *ServerConfig, remote, content string) error {
	return p.WriteRemoteFile(cfg, remote, content)
}

// ConnKeyForTest 暴露连接池键生成规则
func ConnKeyForTest(cfg *ServerConfig) string { return connKey(cfg) }

// StartCleanupLoop 启动空闲连接清理循环（测试可用 SetCleanupTick 控制节奏）
func (p *Pool) StartCleanupLoop() { go p.cleanupLoop() }

// SetCleanupTick 用指定 tick 源驱动清理循环
func (p *Pool) SetCleanupTick(ch <-chan time.Time) { p.cleanupTick = ch }

// InsertConnForTest 直接插入一条无真实会话的连接，用于验证清理/断开等簿记逻辑不因空连接 panic。
// 注意：ActiveKeys 只统计真实 ssh 连接，这类条目不会出现在活跃列表里。
func (p *Pool) InsertConnForTest(cfg *ServerConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns[connKey(cfg)] = &cachedConn{lastUsed: time.Now()}
}

// SetLastUsed 修改某条连接的最后使用时间（模拟空闲连接）
func (p *Pool) SetLastUsed(key string, t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[key]; ok {
		c.lastUsed = t
	}
}

// SweepIdle 立即执行一次空闲连接清理
func (p *Pool) SweepIdle(idle time.Duration) { p.sweepIdle(idle) }

// SetDial 替换真实拨号
func (p *Pool) SetDial(fn func(*ServerConfig) (*ssh.Client, error)) { p.dial = fn }

// SetKeepalive 替换心跳探测
func (p *Pool) SetKeepalive(fn func(*ssh.Client) error) { p.keepalive = fn }

// SetKeepTick 设置心跳间隔
func (p *Pool) SetKeepTick(d time.Duration) { p.keepTick = d }

// SetNewSFTP 替换 SFTP 客户端创建（传 nil 表示该服务器不支持 SFTP，仅命令可用）
func (p *Pool) SetNewSFTP(fn func(*ssh.Client) (*sftp.Client, error)) { p.newSFTP = fn }
