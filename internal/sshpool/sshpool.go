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

// GetClient 获取或复用持久化 SSH 连接
func (p *Pool) GetClient(cfg *ServerConfig) (*ssh.Client, *sftp.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := cfg.ID
	if key == "" {
		key = fmt.Sprintf("%s@%s:%d", cfg.User, cfg.Host, cfg.Port)
	}

	if c, ok := p.conns[key]; ok {
		// 探测连接是否可用
		if _, _, err := c.sshClient.SendRequest("keepalive@buildtodeploy", true, nil); err == nil {
			c.lastUsed = time.Now()
			return c.sshClient, c.sftpClient, nil
		}
		// 连接已断开，关闭旧连接重新连接
		c.close()
		delete(p.conns, key)
	}

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

	// 开启心跳协程，每 25 秒探测一次，保持长连接避免由于空闲被防火墙或 sshd 踢掉
	go func(cli *ssh.Client, stop chan struct{}) {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_, _, _ = cli.SendRequest("keepalive@buildtodeploy", true, nil)
			case <-stop:
				return
			}
		}
	}(client, stopCh)

	p.conns[key] = cached
	return client, sftpCli, nil
}

func dialSSH(cfg *ServerConfig) (*ssh.Client, error) {
	port := cfg.Port
	if port <= 0 {
		port = 22
	}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(port))

	var authMethods []ssh.AuthMethod
	if cfg.AuthType == "key" || cfg.KeyPath != "" {
		keyBytes, err := os.ReadFile(cfg.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("读取私钥失败: %w", err)
		}
		var signer ssh.Signer
		if cfg.Passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(keyBytes, []byte(cfg.Passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(keyBytes)
		}
		if err != nil {
			return nil, fmt.Errorf("解析私钥失败: %w", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	} else {
		authMethods = append(authMethods, ssh.Password(cfg.Password))
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
		return fmt.Errorf("创建 SSH 会话失败: %w", err)
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
		return "", fmt.Errorf("创建 SSH 会话失败: %w", err)
	}
	defer session.Close()

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	done := make(chan error, 1)
	go func() {
		done <- session.Run(cmd)
	}()

	select {
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGKILL)
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
