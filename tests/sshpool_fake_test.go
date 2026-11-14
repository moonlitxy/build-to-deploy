package tests

import (
	"build-to-deploy/internal/sshpool"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// newFakeSSHServer 起一个本地 SSH 服务：握手成功，但拒绝所有全局请求与会话，
// 让客户端 NewSession 失败，从而在不连真机的情况下覆盖连接池的各条分支。
func newFakeSSHServer(t *testing.T) (*ssh.Client, func()) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &ssh.ServerConfig{NoClientAuth: true}
	cfg.AddHostKey(signer)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				sc, chans, reqs, err := ssh.NewServerConn(c, cfg)
				if err != nil {
					return
				}
				defer sc.Close()
				go ssh.DiscardRequests(reqs)
				for newCh := range chans {
					_ = newCh.Reject(ssh.Prohibited, "no sessions")
				}
			}(c)
		}
	}()
	clientCfg := &ssh.ClientConfig{User: "root", Auth: []ssh.AuthMethod{ssh.Password("x")}, HostKeyCallback: ssh.InsecureIgnoreHostKey()}
	client, err := ssh.Dial("tcp", ln.Addr().String(), clientCfg)
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	return client, func() { client.Close(); ln.Close(); wg.Wait() }
}

// fakePool 用假拨号/假 SFTP 构造连接池，拨号计数用于验证"复用 vs 重建"
func fakePool(t *testing.T, keepaliveErr error) (*sshpool.Pool, *int32, *ssh.Client) {
	t.Helper()
	return fakePoolWithTick(t, nil, keepaliveErr)
}

// fakePoolWithTick 在构造期注入清理节奏（循环启动后不再替换，避免数据竞争）
func fakePoolWithTick(t *testing.T, tick <-chan time.Time, keepaliveErr error) (*sshpool.Pool, *int32, *ssh.Client) {
	t.Helper()
	master, closer := newFakeSSHServer(t)
	t.Cleanup(closer)
	var dials int32
	p := sshpool.NewPoolForTest(tick)
	p.SetDial(func(*sshpool.ServerConfig) (*ssh.Client, error) {
		atomic.AddInt32(&dials, 1)
		return master, nil
	})
	// 池只会把 SFTP 句柄原样保存；不需要真实会话，传 nil 即可（close() 对 nil 是安全的）
	p.SetNewSFTP(func(*ssh.Client) (*sftp.Client, error) { return nil, nil })
	p.SetKeepalive(func(*ssh.Client) error { return keepaliveErr })
	return p, &dials, master
}

var fakeCfg = &sshpool.ServerConfig{ID: "fake", Host: "127.0.0.1", Port: 22, User: "root", Password: "x"}

func TestSSHPoolFakeGetClientReuseAndDialBranches(t *testing.T) {
	p, dials, _ := fakePool(t, nil)
	defer p.Disconnect("all")

	if _, _, err := p.GetClient(fakeCfg); err != nil {
		t.Fatal(err)
	}
	// 心跳正常：第二次直接复用，不再拨号
	if _, _, err := p.GetClient(fakeCfg); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(dials) != 1 {
		t.Fatalf("心跳正常时应复用连接，拨号次数 = %d", atomic.LoadInt32(dials))
	}
	if len(p.ActiveKeys()) != 1 {
		t.Fatal("应缓存一条连接")
	}

	// 心跳失败：旧连接被关闭并重连
	p.SetKeepalive(func(*ssh.Client) error { return errors.New("dead") })
	if _, _, err := p.GetClient(fakeCfg); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(dials) != 2 {
		t.Fatalf("心跳失败应重新拨号，拨号次数 = %d", atomic.LoadInt32(dials))
	}
}

func TestSSHPoolFakeGetClientDialErrors(t *testing.T) {
	p := sshpool.NewPool()
	defer p.Disconnect("all")
	p.SetDial(func(*sshpool.ServerConfig) (*ssh.Client, error) { return nil, errors.New("dial refused") })
	if _, _, err := p.GetClient(fakeCfg); err == nil || !strings.Contains(err.Error(), "连接服务器失败") {
		t.Fatalf("拨号失败应包装错误: %v", err)
	}

	// SFTP 初始化失败：关闭刚建立的 SSH 连接并报错
	p, _, _ = fakePool(t, nil)
	p.SetNewSFTP(func(*ssh.Client) (*sftp.Client, error) { return nil, errors.New("sftp boom") })
	// SFTP 不可用不再拖垮命令执行：连接照常缓存，SFTP 句柄为空
	if client, sftpCli, err := p.GetClient(fakeCfg); err != nil || client == nil || sftpCli != nil {
		t.Fatalf("SFTP 不可用时应仅缓存 SSH 连接: %v", err)
	}
	if len(p.ActiveKeys()) != 1 {
		t.Fatal("连接应被缓存")
	}
}

func TestSSHPoolFakeHeartbeatGoroutine(t *testing.T) {
	p, _, master := fakePool(t, nil)
	defer p.Disconnect("all")
	p.SetKeepTick(5 * time.Millisecond)
	if _, _, err := p.GetClient(fakeCfg); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if len(p.ActiveKeys()) != 1 {
		t.Fatal("心跳成功时连接应保持")
	}

	// 连续失败：心跳协程主动剔除失效连接
	bad := sshpool.NewPool()
	defer bad.Disconnect("all")
	bad.SetKeepTick(2 * time.Millisecond)
	bad.SetDial(func(*sshpool.ServerConfig) (*ssh.Client, error) { return master, nil })
	bad.SetNewSFTP(func(*ssh.Client) (*sftp.Client, error) { return nil, nil })
	bad.SetKeepalive(func(*ssh.Client) error { return errors.New("dead") })
	if _, _, err := bad.GetClient(fakeCfg); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(bad.ActiveKeys()) != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if len(bad.ActiveKeys()) != 0 {
		t.Fatal("心跳连续失败后应剔除连接")
	}
}

func TestSSHPoolFakeSessionOperationsAgainstFakeServer(t *testing.T) {
	p, _, _ := fakePool(t, nil)
	defer p.Disconnect("all")
	ctx := context.Background()
	// 假服务拒绝会话：验证"取连接成功但建会话失败"的分支
	if err := p.TestConnection(fakeCfg); err == nil || !strings.Contains(err.Error(), "创建会话失败") {
		t.Fatalf("TestConnection = %v", err)
	}
	if err := p.RunCommand(ctx, fakeCfg, "echo hi", io.Discard); err == nil {
		t.Fatal("RunCommand 建会话失败应报错")
	}
	if _, err := p.ExecuteOutput(ctx, fakeCfg, "echo hi"); err == nil {
		t.Fatal("ExecuteOutput 建会话失败应报错")
	}
	// 会话失败后已剔除旧连接并重连；再次失败时返回"创建 SSH 会话失败"
	// 会话失败后旧连接已从池中摘除，下一次调用需要重新拨号；拨号失败时直接返回"连接服务器失败"
	p.SetDial(func(*sshpool.ServerConfig) (*ssh.Client, error) { return nil, errors.New("gone") })
	if err := p.RunCommand(ctx, fakeCfg, "x", io.Discard); err == nil || !strings.Contains(err.Error(), "连接服务器失败") {
		t.Fatalf("重连失败 = %v", err)
	}
	if _, err := p.ExecuteOutput(ctx, fakeCfg, "x"); err == nil || !strings.Contains(err.Error(), "连接服务器失败") {
		t.Fatalf("重连失败 = %v", err)
	}
}

func TestSSHPoolFakeSweepIdleAndClose(t *testing.T) {
	p, _, _ := fakePool(t, nil)
	defer p.Disconnect("all")
	if _, _, err := p.GetClient(fakeCfg); err != nil {
		t.Fatal(err)
	}
	p.SetLastUsed("fake", time.Now().Add(-time.Hour))
	p.SweepIdle(30 * time.Minute)
	if len(p.ActiveKeys()) != 0 {
		t.Fatal("空闲连接应被清理")
	}
	// close 的幂等性：重复关闭已断开的连接不应 panic
	p.InsertConnForTest(fakeCfg)
	p.Disconnect(fakeCfg.ID)
	p.Disconnect(fakeCfg.ID)
}

// newSessionSSHServer 假服务：接受 exec 请求，按 behave 决定输出与退出码
type sessionBehavior struct {
	stdout string
	stderr string
	exit   uint32
	hang   bool // 不响应，模拟卡住的远端命令
}

func newSessionSSHServer(t *testing.T, b sessionBehavior) (int, func()) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &ssh.ServerConfig{NoClientAuth: true}
	cfg.AddHostKey(signer)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer c.Close()
				sc, chans, reqs, err := ssh.NewServerConn(c, cfg)
				if err != nil {
					return
				}
				defer sc.Close()
				go ssh.DiscardRequests(reqs)
				for newCh := range chans {
					if newCh.ChannelType() != "session" {
						_ = newCh.Reject(ssh.UnknownChannelType, "unsupported")
						continue
					}
					ch, requests, err := newCh.Accept()
					if err != nil {
						continue
					}
					// 外层必须继续读 chans，否则后续请求（如 sftp 子系统）会一直阻塞在这里
					go func(ch ssh.Channel, requests <-chan *ssh.Request) {
						defer ch.Close()
						for req := range requests {
							if req.Type == "subsystem" {
								// 不实现 SFTP：客户端 NewClient 会探测失败（silent），行为可预期
								_ = req.Reply(false, nil)
								continue
							}
							if req.Type != "exec" {
								_ = req.Reply(false, nil)
								continue
							}
							_ = req.Reply(true, nil)
							if b.hang {
								continue // 吞掉请求：命令永远不结束
							}
							ch.Write([]byte(b.stdout))
							ch.Stderr().Write([]byte(b.stderr))
							status := ssh.Marshal(struct{ Status uint32 }{b.exit})
							_, _ = ch.SendRequest("exit-status", false, status)
							ch.Close()
							return
						}
					}(ch, requests)
				}
			}(c)
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return addr.Port, func() {
		ln.Close()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			// 挂起用例的连接协程仍在等待，不阻塞测试收尾
		}
	}
}

func TestSSHPoolFakeSessionExecSuccessAndFailure(t *testing.T) {
	port, closer := newSessionSSHServer(t, sessionBehavior{stdout: "hello out\n", stderr: "", exit: 0})
	defer closer()
	cfg := &sshpool.ServerConfig{ID: "ok", Host: "127.0.0.1", Port: port, User: "root", Password: "x"}
	p := sshpool.NewPool()
	p.SetNewSFTP(func(*ssh.Client) (*sftp.Client, error) { return nil, nil }) // 假服务不实现 SFTP
	defer p.Disconnect("all")
	ctx := context.Background()

	out, err := p.ExecuteOutput(ctx, cfg, "echo hi")
	if err != nil || out != "hello out" {
		t.Fatalf("ExecuteOutput = %q, %v", out, err)
	}
	if err := p.TestConnection(cfg); err != nil {
		t.Fatalf("TestConnection = %v", err)
	}
	var buf bytes.Buffer
	if err := p.RunCommand(ctx, cfg, "echo hi", &buf); err != nil {
		t.Fatalf("RunCommand = %v", err)
	}
	if !strings.Contains(buf.String(), "$ echo hi") || !strings.Contains(buf.String(), "hello out") {
		t.Fatalf("RunCommand 输出 = %q", buf.String())
	}

	// 非零退出码：优先返回 stderr 文本
	port2, closer2 := newSessionSSHServer(t, sessionBehavior{stdout: "partial", stderr: "boom\n", exit: 1})
	defer closer2()
	cfg2 := &sshpool.ServerConfig{ID: "bad", Host: "127.0.0.1", Port: port2, User: "root", Password: "x"}
	out, err = p.ExecuteOutput(ctx, cfg2, "false")
	if err == nil || err.Error() != "boom" || out != "partial" {
		t.Fatalf("失败结果 = %q, %v", out, err)
	}
	// stderr 为空时退回错误字符串
	port3, closer3 := newSessionSSHServer(t, sessionBehavior{exit: 2})
	defer closer3()
	cfg3 := &sshpool.ServerConfig{ID: "bad2", Host: "127.0.0.1", Port: port3, User: "root", Password: "x"}
	if _, err := p.ExecuteOutput(ctx, cfg3, "x"); err == nil {
		t.Fatal("退出码非 0 应报错")
	}
}

func TestSSHPoolFakeExecuteOutputCanceled(t *testing.T) {
	port, closer := newSessionSSHServer(t, sessionBehavior{hang: true})
	defer closer()
	cfg := &sshpool.ServerConfig{ID: "hang", Host: "127.0.0.1", Port: port, User: "root", Password: "x"}
	p := sshpool.NewPool()
	p.SetNewSFTP(func(*ssh.Client) (*sftp.Client, error) { return nil, nil })
	defer p.Disconnect("all")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := p.ExecuteOutput(ctx, cfg, "sleep 100")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("取消应返回上下文错误: %v", err)
	}
}

func TestSSHPoolFakeDialSSHKeyAuth(t *testing.T) {
	port, closer := newSessionSSHServer(t, sessionBehavior{stdout: "ok"})
	defer closer()
	dir := t.TempDir()
	// 受口令保护的私钥：提供口令可解析，错误口令必须报错
	keyPath := filepath.Join(dir, "id")
	pass := "passphrase"
	if err := genEncryptedKey(keyPath, pass); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSH_AUTH_SOCK", "")
	cfg := &sshpool.ServerConfig{Host: "127.0.0.1", Port: port, User: "root", AuthType: "key", KeyPath: keyPath, Passphrase: pass}
	// 服务端 NoClientAuth：认证方式只要能构造出来就会握手成功
	if _, err := sshpool.DialForTest(cfg); err != nil {
		t.Fatalf("带口令私钥应能解析: %v", err)
	}
	bad := *cfg
	bad.Passphrase = "wrong"
	if _, err := sshpool.DialForTest(&bad); err == nil {
		t.Fatal("错误口令应报错")
	}
	// 未提供口令时解析受保护私钥失败
	noPass := *cfg
	noPass.Passphrase = ""
	if _, err := sshpool.DialForTest(&noPass); err == nil {
		t.Fatal("缺少口令应报错")
	}
	// ~ 展开：指向不存在文件时报读取错误
	if _, err := sshpool.DialForTest(&sshpool.ServerConfig{Host: "127.0.0.1", Port: port, User: "root", AuthType: "key", KeyPath: "~/__definitely_missing_key__"}); err == nil {
		t.Fatal("不存在的 ~ 路径应报错")
	}
}

// genEncryptedKey 生成一把带口令的 Ed25519 私钥
func genEncryptedKey(path, passphrase string) error {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	block, err := ssh.MarshalPrivateKeyWithPassphrase(priv, "", []byte(passphrase))
	if err != nil {
		return err
	}
	return os.WriteFile(path, pem.EncodeToMemory(block), 0o600)
}

func TestSSHPoolFakeSendKeepaliveRealMux(t *testing.T) {
	master, closer := newFakeSSHServer(t)
	defer closer()
	// 假服务用 DiscardRequests（wantReply=false 的全局请求不回 reply），此处直接验证函数不 panic 且能返回
	_ = sshpool.SendKeepaliveForTest(master)
}

func TestSSHPoolFakeDialSSHAgentAndDefaultKeyProbe(t *testing.T) {
	_, closer := newFakeSSHServer(t)
	defer closer()
	// 不显式指定认证方式且没有密码/私钥：会尝试读本地 SSH Agent，随后走默认私钥探测，最终连接失败但分支被覆盖
	t.Setenv("SSH_AUTH_SOCK", "")
	if _, err := sshpool.DialForTest(&sshpool.ServerConfig{Host: "127.0.0.1", Port: 1, User: "root", AuthType: "agent"}); err == nil {
		t.Fatal("没有可用认证方式时应失败")
	}
	// 端口为 0 时按 22 处理：host 不可达也会走到拨号
	if _, err := sshpool.DialForTest(&sshpool.ServerConfig{Host: "127.0.0.1", Port: 0, User: "root", Password: "x"}); err == nil {
		t.Fatal("不可达端口应失败")
	}
}

func TestSSHPoolFakeCleanupLoopSweeps(t *testing.T) {
	tick := make(chan time.Time)
	p, _, _ := fakePoolWithTick(t, tick, nil)
	go p.StartCleanupLoop()
	if _, _, err := p.GetClient(fakeCfg); err != nil {
		t.Fatal(err)
	}
	p.SetLastUsed("fake", time.Now().Add(-time.Hour))
	tick <- time.Now()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(p.ActiveKeys()) != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if len(p.ActiveKeys()) != 0 {
		t.Fatal("清理循环应清掉空闲连接")
	}
	close(tick)
}

// 没有真实会话的连接：清理/断开时必须安全忽略，且不计入活跃连接
func TestSSHPoolSimulatedConnSweep(t *testing.T) {
	p := sshpool.NewPool()
	defer p.Disconnect("all")
	p.InsertConnForTest(fakeCfg)
	p.SetLastUsed("fake", time.Now().Add(-2*time.Hour))
	p.SweepIdle(30 * time.Minute)
	if len(p.ActiveKeys()) != 0 {
		t.Fatal("模拟连接空闲后应被清理")
	}
	p.InsertConnForTest(fakeCfg)
	if n := p.Disconnect("all"); n != 1 {
		t.Fatalf("应断开 1 条模拟连接，实际 %d", n)
	}
}
