package sshpool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// ServerConfig SSH 服务器连接配置
type ServerConfig struct {
	ID         string `json:"id" yaml:"id"`
	Name       string `json:"name" yaml:"name"`
	Host       string `json:"host" yaml:"host"`
	Port       int    `json:"port" yaml:"port"`
	User       string `json:"user" yaml:"user"`
	AuthType   string `json:"authType" yaml:"authType"` // "password" 或 "key"
	Password   string `json:"password" yaml:"password"`
	KeyPath    string `json:"keyPath" yaml:"keyPath"`
	Passphrase string `json:"passphrase" yaml:"passphrase"`
}

// ShellQuote 将变量安全地拼入远程 sh 命令：单引号包裹并转义内部单引号。
// 所有会进入远程 shell 的变量都必须经过它。
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

type cachedConn struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	lastUsed   time.Time
	stopKeep   chan struct{}
}

// Pool SSH 客户端连接池，保持常驻心跳，避免频繁建立 SSH 握手触发服务器连接限制
type Pool struct {
	mu    sync.Mutex
	conns map[string]*cachedConn
}

var GlobalPool = NewPool()

func NewPool() *Pool {
	p := &Pool{
		conns: make(map[string]*cachedConn),
	}
	// 定时清理超过 30 分钟未使用的连接
	go p.cleanupLoop()
	return p
}

func (p *Pool) cleanupLoop() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		p.mu.Lock()
		now := time.Now()
		for id, conn := range p.conns {
			if now.Sub(conn.lastUsed) > 30*time.Minute {
				conn.close()
				delete(p.conns, id)
			}
		}
		p.mu.Unlock()
	}
}

func (c *cachedConn) close() {
	if c.stopKeep != nil {
		close(c.stopKeep)
		c.stopKeep = nil
	}
	if c.sftpClient != nil {
		_ = c.sftpClient.Close()
	}
	if c.sshClient != nil {
		_ = c.sshClient.Close()
	}
}

// GetClient 获取或复用持久化 SSH 连接。
// 探测与拨号都在锁外进行：某台服务器不可达时不会阻塞其它服务器的请求与连接清理。
func (p *Pool) GetClient(cfg *ServerConfig) (*ssh.Client, *sftp.Client, error) {
	key := connKey(cfg)

	p.mu.Lock()
	if c, ok := p.conns[key]; ok && c.sshClient != nil {
		// 探测连接是否可用
		if _, _, err := c.sshClient.SendRequest("keepalive@buildtodeploy", true, nil); err == nil {
			c.lastUsed = time.Now()
			client, sftpCli := c.sshClient, c.sftpClient
			p.mu.Unlock()
			return client, sftpCli, nil
		}
		// 连接已断开，关闭旧连接重新连接
		c.close()
		delete(p.conns, key)
	}
	p.mu.Unlock()

	client, err := dialSSH(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("连接服务器失败 [%s@%s:%d]: %w", cfg.User, cfg.Host, cfg.Port, err)
	}

	sftpCli, err := sftp.NewClient(client)
	if err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("初始化 SFTP 失败: %w", err)
	}

	stopCh := make(chan struct{})
	cached := &cachedConn{
		sshClient:  client,
		sftpClient: sftpCli,
		lastUsed:   time.Now(),
		stopKeep:   stopCh,
	}

	p.mu.Lock()
	// 并发拨号时可能已有其它 goroutine 先建立连接：复用它，关闭本次新建的连接
	if old, ok := p.conns[key]; ok && old.sshClient != nil {
		old.lastUsed = time.Now()
		existingClient, existingSFTP := old.sshClient, old.sftpClient
		p.mu.Unlock()
		_ = sftpCli.Close()
		_ = client.Close()
		close(stopCh)
		return existingClient, existingSFTP, nil
	}
	p.conns[key] = cached
	p.mu.Unlock()

	// 开启心跳协程，每 25 秒探测一次，保持长连接避免由于空闲被防火墙或 sshd 踢掉
	go func(connKey string, cli *ssh.Client, stop chan struct{}) {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		failCount := 0
		for {
			select {
			case <-t.C:
				_, _, err := cli.SendRequest("keepalive@buildtodeploy", true, nil)
				if err != nil {
					failCount++
					if failCount >= 2 {
						// 连续 2 次心跳失败，说明连接确已断开，主动清理剔除
						p.mu.Lock()
						if c, ok := p.conns[connKey]; ok && c.sshClient == cli {
							c.close()
							delete(p.conns, connKey)
						}
						p.mu.Unlock()
						return
					}
				} else {
					failCount = 0
				}
			case <-stop:
				return
			}
		}
	}(key, client, stopCh)

	return client, sftpCli, nil
}

// connKey 连接池键：优先使用服务器 ID
func connKey(cfg *ServerConfig) string {
	if cfg.ID != "" {
		return cfg.ID
	}
	return fmt.Sprintf("%s@%s:%d", cfg.User, cfg.Host, cfg.Port)
}

// Invalidate 主动清理并断开指定服务器的缓存连接
func (p *Pool) Invalidate(cfg *ServerConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := cfg.ID
	if key == "" {
		key = fmt.Sprintf("%s@%s:%d", cfg.User, cfg.Host, cfg.Port)
	}
	if c, ok := p.conns[key]; ok {
		c.close()
		delete(p.conns, key)
	}
}

// Disconnect 主动断开指定服务器（或全部）长连接，释放本地网络资源
func (p *Pool) Disconnect(serverID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	if serverID == "" || serverID == "all" {
		for key, c := range p.conns {
			c.close()
			delete(p.conns, key)
			count++
		}
		return count
	}

	for key, c := range p.conns {
		if key == serverID || strings.HasPrefix(key, serverID+"@") {
			c.close()
			delete(p.conns, key)
			count++
		}
	}
	return count
}

// ActiveKeys 返回当前正在保活的长连接列表
func (p *Pool) ActiveKeys() []string {
	// 清理协程会并发关闭连接，锁内完成快照
	p.mu.Lock()
	defer p.mu.Unlock()
	var keys []string
	for k := range p.conns {
		keys = append(keys, k)
	}
	return keys
}

// MarkSimulated 在演练模式 (DryRun) 下标记服务器长连接已建立，以便前端状态一致
func (p *Pool) MarkSimulated(cfg *ServerConfig) {
	if cfg == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := cfg.ID
	if key == "" {
		key = fmt.Sprintf("%s@%s:%d", cfg.User, cfg.Host, cfg.Port)
	}
	p.conns[key] = &cachedConn{
		lastUsed: time.Now(),
	}
}

func dialSSH(cfg *ServerConfig) (*ssh.Client, error) {
	port := cfg.Port
	if port <= 0 {
		port = 22
	}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(port))

	var authMethods []ssh.AuthMethod

	// 1. 仅在显式选择 agent 认证时使用本地 SSH Agent，避免把本机密钥交给任意新添加的服务器
	useAgent := cfg.AuthType == "agent" || (cfg.AuthType == "" && cfg.Password == "" && cfg.KeyPath == "")
	if sock := os.Getenv("SSH_AUTH_SOCK"); useAgent && sock != "" {
		if agentConn, err := net.Dial("unix", sock); err == nil {
			agentCli := agent.NewClient(agentConn)
			var once sync.Once
			authMethods = append(authMethods, ssh.PublicKeysCallback(func() ([]ssh.Signer, error) {
				signers, err := agentCli.Signers()
				// 认证阶段结束后关闭 agent 连接，避免每次重连泄漏文件描述符
				once.Do(func() { _ = agentConn.Close() })
				return signers, err
			}))
		}
	}

	// 2. 处理私钥认证
	keyPath := cfg.KeyPath
	if strings.HasPrefix(keyPath, "~/") || strings.HasPrefix(keyPath, "~\\") {
		if home, err := os.UserHomeDir(); err == nil {
			keyPath = filepath.Join(home, keyPath[2:])
		}
	}
	// 如果选择 key 鉴权但未填路径，自动探测默认私钥
	if (cfg.AuthType == "key" || cfg.AuthType == "agent") && keyPath == "" {
		if home, err := os.UserHomeDir(); err == nil {
			for _, defKey := range []string{".ssh/id_ed25519", ".ssh/id_rsa"} {
				p := filepath.Join(home, defKey)
				if _, err := os.Stat(p); err == nil {
					keyPath = p
					break
				}
			}
		}
	}

	if keyPath != "" {
		// 私钥读取/口令错误必须显式报错：静默跳过后只会得到误导性的 "no supported methods remain"
		keyBytes, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("读取私钥文件 (%s) 失败: %w", keyPath, err)
		}
		var signer ssh.Signer
		if cfg.Passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(keyBytes, []byte(cfg.Passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(keyBytes)
		}
		if err != nil {
			return nil, fmt.Errorf("解析私钥文件 (%s) 失败（口令错误或私钥受口令保护）: %w", keyPath, err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	// 3. 密码鉴权
	if cfg.Password != "" {
		authMethods = append(authMethods, ssh.Password(cfg.Password))
	}

	if len(authMethods) == 0 {
		return nil, errors.New("未配置有效的 SSH 认证方式（请提供密码、私钥或开启 SSH Agent）")
	}

	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 内网/VPN环境
		Timeout:         10 * time.Second,
	}

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	sshConn, chans, reqs, err := ssh.NewClientConn(conn, addr, sshConfig)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return ssh.NewClient(sshConn, chans, reqs), nil
}

// TestConnection 测试 SSH 连接
func (p *Pool) TestConnection(cfg *ServerConfig) error {
	client, _, err := p.GetClient(cfg)
	if err != nil {
		return err
	}
	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("创建会话失败: %w", err)
	}
	defer session.Close()
	return session.Run("uname -a")
}

// RunCommand 远程执行命令，实时将输出写入 writer
func (p *Pool) RunCommand(ctx context.Context, cfg *ServerConfig, cmd string, w io.Writer) error {
	client, _, err := p.GetClient(cfg)
	if err != nil {
		return err
	}
	session, err := client.NewSession()
	if err != nil {
		// 旧长连接可能在网络抖动中已断开，自愈剔除并重新建立连接重试一次
		p.Invalidate(cfg)
		client, _, err = p.GetClient(cfg)
		if err != nil {
			return fmt.Errorf("重新连接 SSH 失败: %w", err)
		}
		session, err = client.NewSession()
		if err != nil {
			return fmt.Errorf("创建 SSH 会话失败: %w", err)
		}
	}
	defer session.Close()

	session.Stdout = w
	session.Stderr = w

	done := make(chan error, 1)
	go func() {
		fmt.Fprintf(w, "$ %s\n", cmd)
		done <- session.Run(cmd)
	}()

	select {
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGKILL)
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// ExecuteOutput 执行远程命令并捕获字符串输出
func (p *Pool) ExecuteOutput(ctx context.Context, cfg *ServerConfig, cmd string) (string, error) {
	client, _, err := p.GetClient(cfg)
	if err != nil {
		return "", err
	}
	session, err := client.NewSession()
	if err != nil {
		// 旧长连接失效自愈重试一次
		p.Invalidate(cfg)
		client, _, err = p.GetClient(cfg)
		if err != nil {
			return "", fmt.Errorf("重新连接 SSH 失败: %w", err)
		}
		session, err = client.NewSession()
		if err != nil {
			return "", fmt.Errorf("创建 SSH 会话失败: %w", err)
		}
	}
	defer session.Close()

	var stdout, stderr syncBuffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	done := make(chan error, 1)
	go func() {
		done <- session.Run(cmd)
	}()

	select {
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGKILL)
		// 等会话真正结束再返回，避免读到被并发追加的 buffer；但远端不响应信号时不能无限等
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = session.Close()
		}
		return "", ctx.Err()
	case err := <-done:
		if err != nil {
			errStr := strings.TrimSpace(stderr.String())
			if errStr == "" {
				errStr = err.Error()
			}
			return strings.TrimSpace(stdout.String()), errors.New(errStr)
		}
		return strings.TrimSpace(stdout.String()), nil
	}
}

// syncBuffer 供 SSH 会话后台读取协程并发写入时使用（bytes.Buffer 非并发安全）
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// UploadFile 通过长连接 SFTP 将本地文件上传到远程服务器的指定路径
func (p *Pool) UploadFile(ctx context.Context, cfg *ServerConfig, localPath string, remotePath string, progressFn func(uploaded, total int64)) error {
	_, sftpCli, err := p.GetClient(cfg)
	if err != nil {
		return err
	}

	localFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("打开本地文件失败: %w", err)
	}
	defer localFile.Close()

	stat, err := localFile.Stat()
	if err != nil {
		return err
	}
	totalSize := stat.Size()

	// 远程创建目标目录
	remoteDir := filepath.Dir(remotePath)
	// 在 linux 环境下路径统一转为 /
	remoteDir = filepath.ToSlash(remoteDir)
	remotePath = filepath.ToSlash(remotePath)

	if err := sftpCli.MkdirAll(remoteDir); err != nil {
		return fmt.Errorf("创建远程目录 %s 失败: %w", remoteDir, err)
	}

	remoteFile, err := sftpCli.Create(remotePath)
	if err != nil {
		return fmt.Errorf("创建远程文件 %s 失败: %w", remotePath, err)
	}
	defer remoteFile.Close()

	buf := make([]byte, 128*1024) // 128KB 缓冲区提高 SFTP 吞吐
	var uploaded int64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := localFile.Read(buf)
		if n > 0 {
			if _, writeErr := remoteFile.Write(buf[:n]); writeErr != nil {
				return fmt.Errorf("写入远程文件失败: %w", writeErr)
			}
			uploaded += int64(n)
			if progressFn != nil {
				progressFn(uploaded, totalSize)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	return nil
}

// maxRemoteRead 读取远程文本文件的大小上限（K8s YAML 等），防止误指向大文件时耗尽内存
const maxRemoteRead = 8 << 20

// ReadRemoteFile 通过 SFTP 读取远程服务器上的文本文件内容
func (p *Pool) ReadRemoteFile(cfg *ServerConfig, remotePath string) (string, error) {
	_, sftpCli, err := p.GetClient(cfg)
	if err != nil {
		return "", err
	}
	remotePath = filepath.ToSlash(remotePath)
	f, err := sftpCli.Open(remotePath)
	if err != nil {
		return "", fmt.Errorf("打开远程文件 %s 失败: %w", remotePath, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxRemoteRead+1))
	if err != nil {
		return "", fmt.Errorf("读取远程文件 %s 失败: %w", remotePath, err)
	}
	if int64(len(b)) > maxRemoteRead {
		return "", fmt.Errorf("远程文件 %s 超过 %d MB 上限，拒绝读取", remotePath, maxRemoteRead>>20)
	}
	return string(b), nil
}

// WriteRemoteFile 通过 SFTP 将修改后的文本内容写回远程服务器文件。
// 先写临时文件再重命名，避免写入中途断开导致原文件被截断。
func (p *Pool) WriteRemoteFile(cfg *ServerConfig, remotePath string, content string) error {
	_, sftpCli, err := p.GetClient(cfg)
	if err != nil {
		return err
	}
	remotePath = filepath.ToSlash(remotePath)
	mode := os.FileMode(0o644)
	if st, err := sftpCli.Stat(remotePath); err == nil {
		mode = st.Mode().Perm()
	}
	tmpPath := remotePath + ".tmp"
	f, err := sftpCli.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("打开远程文件 %s 写入失败: %w", tmpPath, err)
	}
	if _, err := f.Write([]byte(content)); err != nil {
		_ = f.Close()
		_ = sftpCli.Remove(tmpPath)
		return fmt.Errorf("写入远程文件 %s 失败: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		_ = sftpCli.Remove(tmpPath)
		return fmt.Errorf("关闭远程文件 %s 失败: %w", tmpPath, err)
	}
	_ = sftpCli.Chmod(tmpPath, mode)
	// 优先使用 posix-rename@openssh.com 扩展（可原子覆盖），不支持时回退普通 rename
	if err := sftpCli.PosixRename(tmpPath, remotePath); err != nil {
		if err := sftpCli.Rename(tmpPath, remotePath); err != nil {
			_ = sftpCli.Remove(tmpPath)
			return fmt.Errorf("替换远程文件 %s 失败: %w", remotePath, err)
		}
	}
	return nil
}
