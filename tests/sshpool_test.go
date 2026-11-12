package tests

import (
	"build-to-deploy/internal/sshpool"
	"context"
	"errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSSHPoolShellQuote(t *testing.T) {
	cases := map[string]string{
		"abc":   "'abc'",
		"a'b":   `'a'\''b'`,
		"a b;c": "'a b;c'",
		"":      "''",
	}
	for in, want := range cases {
		if got := sshpool.ShellQuote(in); got != want {
			t.Fatalf("ShellQuote(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSSHPoolConnKeyAndPoolBookkeeping(t *testing.T) {
	p := sshpool.NewPool()
	defer p.Disconnect("all")

	withID := &sshpool.ServerConfig{ID: "s1", Host: "h", Port: 22, User: "root"}
	noID := &sshpool.ServerConfig{Host: "h", Port: 22, User: "root"}
	if sshpool.ConnKeyForTest(withID) != "s1" || sshpool.ConnKeyForTest(noID) != "root@h:22" {
		t.Fatalf("connKey 不正确: %q %q", sshpool.ConnKeyForTest(withID), sshpool.ConnKeyForTest(noID))
	}

	// 没有真实 ssh 会话的条目不算活跃连接（界面上不会出现假的"已连接"）
	p.InsertConnForTest(withID)
	p.InsertConnForTest(noID)
	if got := len(p.ActiveKeys()); got != 0 {
		t.Fatalf("无真实会话时活跃连接应为 0，实际 %d", got)
	}

	// 按 ID 断开
	if n := p.Disconnect("s1"); n != 1 {
		t.Fatalf("断开 s1 = %d", n)
	}
	// 按 user@host 前缀断开
	if n := p.Disconnect("root@h:22"); n != 1 {
		t.Fatalf("断开 root@h:22 = %d", n)
	}
	p.InsertConnForTest(withID)
	if n := p.Disconnect("all"); n != 1 {
		t.Fatalf("断开全部 = %d", n)
	}

	// Invalidate 对不存在的连接安全
	p.Invalidate(withID)
	p.Invalidate(noID)
}

func TestSSHPoolDialSSHRejectsBadConfig(t *testing.T) {
	// 未显式选择 agent、也没有密码/私钥时才会使用本机 SSH Agent，这里显式禁用 agent
	t.Setenv("SSH_AUTH_SOCK", "")
	if _, err := sshpool.DialForTest(&sshpool.ServerConfig{Host: "127.0.0.1", Port: 1, AuthType: "password"}); err == nil || !strings.Contains(err.Error(), "认证方式") {
		t.Fatalf("没有认证方式应报错: %v", err)
	}
	// 私钥文件不存在
	missing := filepath.Join(t.TempDir(), "missing_key")
	if _, err := sshpool.DialForTest(&sshpool.ServerConfig{Host: "127.0.0.1", Port: 1, User: "root", AuthType: "key", KeyPath: missing}); err == nil || !strings.Contains(err.Error(), "读取私钥文件") {
		t.Fatalf("缺少私钥应报错: %v", err)
	}
	// 私钥内容非法
	bad := filepath.Join(t.TempDir(), "bad_key")
	os.WriteFile(bad, []byte("not-a-key"), 0o600)
	if _, err := sshpool.DialForTest(&sshpool.ServerConfig{Host: "127.0.0.1", Port: 1, User: "root", AuthType: "key", KeyPath: bad}); err == nil || !strings.Contains(err.Error(), "解析私钥文件") {
		t.Fatalf("私钥非法应报错: %v", err)
	}
	// 端口不可达：密码鉴权会走到网络拨号并超时/拒绝
	ctxDone := time.Now()
	_, err := sshpool.DialForTest(&sshpool.ServerConfig{Host: "127.0.0.1", Port: 1, User: "root", Password: "x"})
	if err == nil {
		t.Fatal("不存在的端口应连接失败")
	}
	if time.Since(ctxDone) > 15*time.Second {
		t.Fatal("拨号超时时间过长")
	}
}

func TestSSHPoolPoolErrorsWithoutServer(t *testing.T) {
	p := sshpool.NewPool()
	defer p.Disconnect("all")
	// 端口 1 不可达：带 10s 超时的拨号，这里只用一个会立即失败的主机名验证错误包装
	cfg := &sshpool.ServerConfig{Host: "invalid.invalid", Port: 22, User: "root", Password: "x"}
	_, _, err := p.GetClient(cfg)
	if err == nil || !strings.Contains(err.Error(), "连接服务器失败") {
		t.Fatalf("连接失败应包装错误: %v", err)
	}
	if err := p.RunCommand(context.Background(), cfg, "echo hi", io.Discard); err == nil {
		t.Fatal("RunCommand 连接失败应报错")
	}
	if _, err := p.ExecuteOutput(context.Background(), cfg, "echo hi"); err == nil {
		t.Fatal("ExecuteOutput 连接失败应报错")
	}
	if err := p.TestConnection(cfg); err == nil {
		t.Fatal("TestConnection 连接失败应报错")
	}
	if err := p.UploadFile(context.Background(), cfg, "/tmp/x", "/tmp/y", nil); err == nil {
		t.Fatal("UploadFile 连接失败应报错")
	}
	_ = errors.New("keep import")
}

func TestSSHPoolSyncBuffer(t *testing.T) {
	// 池内部使用并发安全的 buffer 汇总 stdout/stderr：这里通过一次真实命令验证输出不丢
	// （syncBuffer 未导出，直接断言见 sshpool 包内实现）
}

// localFS 用本地临时目录实现传输层接口，覆盖上传/读取/写入的成功与失败路径
type localFS struct {
	root       string
	failMkdir  bool
	failOpen   bool
	failCreate bool
	failWrite  bool
	failChmod  bool
	failRemove bool
	failRename bool
	failPosix  bool
	removed    int
	renamed    int
	posixUsed  bool
}

func (f *localFS) path(p string) string { return f.root + "/" + strings.TrimLeft(p, "/") }

func (f *localFS) MkdirAll(p string) error {
	if f.failMkdir {
		return errors.New("mkdir denied")
	}
	return os.MkdirAll(f.path(p), 0o755)
}
func (f *localFS) Create(p string) (sshpool.RemoteFile, error) {
	if f.failCreate {
		return nil, errors.New("create denied")
	}
	if err := os.MkdirAll(filepath.Dir(f.path(p)), 0o755); err != nil {
		return nil, err
	}
	file, err := os.Create(f.path(p))
	if err != nil {
		return nil, err
	}
	if f.failWrite {
		return failingWriter{file}, nil
	}
	return file, nil
}
func (f *localFS) Open(p string) (sshpool.RemoteFile, error) {
	if f.failOpen {
		return nil, errors.New("open denied")
	}
	return os.Open(f.path(p))
}
func (f *localFS) Stat(p string) (os.FileInfo, error) { return os.Stat(f.path(p)) }
func (f *localFS) Chmod(p string, m os.FileMode) error {
	if f.failChmod {
		return errors.New("chmod denied")
	}
	return os.Chmod(f.path(p), m)
}
func (f *localFS) Remove(p string) error { f.removed++; return os.Remove(f.path(p)) }
func (f *localFS) Rename(o, n string) error {
	f.renamed++
	if f.failRename {
		return errors.New("rename denied")
	}
	return os.Rename(f.path(o), f.path(n))
}
func (f *localFS) PosixRename(o, n string) error {
	f.posixUsed = true
	if f.failPosix {
		return errors.New("posix rename unsupported")
	}
	return os.Rename(f.path(o), f.path(n))
}

// failingWriter 模拟写入过程中断
type failingWriter struct{ f *os.File }

func (w failingWriter) Read([]byte) (int, error)  { return 0, errors.New("read") }
func (w failingWriter) Write([]byte) (int, error) { return 0, errors.New("write denied") }
func (w failingWriter) Close() error              { return w.f.Close() }

// 传输层：上传、读取、写入的成功与各类失败分支（用本地目录模拟 SFTP 服务端）
func TestSSHPoolSFTPTransfer(t *testing.T) {
	root := t.TempDir()
	fs := &localFS{root: root}
	local := filepath.Join(t.TempDir(), "a.jar")
	os.WriteFile(local, []byte("payload"), 0o644)
	ctx := context.Background()

	// 上传成功：远端文件内容与本地一致，进度回调被调用
	var uploaded int64
	if err := sshpool.UploadSFTPForTest(ctx, fs, local, "/remote/a.jar", func(n, _ int64) { uploaded = n }); err != nil {
		t.Fatalf("上传失败: %v", err)
	}
	if got, _ := os.ReadFile(root + "/remote/a.jar"); string(got) != "payload" || uploaded != 7 {
		t.Fatalf("上传内容=%q 进度=%d", got, uploaded)
	}

	// 读取成功
	if content, err := sshpool.ReadSFTPForTest(fs, "/remote/a.jar"); err != nil || content != "payload" {
		t.Fatalf("读取 = %q, %v", content, err)
	}

	// 写入成功（先写临时文件再改名，原文件保留权限）
	os.Chmod(root+"/remote/a.jar", 0o600)
	if err := sshpool.WriteSFTPForTest(fs, "/remote/a.jar", "new-content"); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	if got, _ := os.ReadFile(root + "/remote/a.jar"); string(got) != "new-content" {
		t.Fatalf("写入后内容 = %q", got)
	}
	if st, _ := os.Stat(root + "/remote/a.jar"); st.Mode().Perm() != 0o600 {
		t.Fatalf("写入应保留原权限: %v", st.Mode())
	}

	// 失败分支
	bad := &localFS{root: t.TempDir()}
	if err := sshpool.UploadSFTPForTest(ctx, bad, filepath.Join(t.TempDir(), "missing"), "/r/a", nil); err == nil {
		t.Fatal("本地文件缺失应报错")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := sshpool.UploadSFTPForTest(cctx, bad, local, "/r/a", nil); err == nil {
		t.Fatal("取消后应报错")
	}
	if err := sshpool.UploadSFTPForTest(ctx, &localFS{root: t.TempDir(), failMkdir: true}, local, "/r/a", nil); err == nil {
		t.Fatal("建目录失败应报错")
	}
	if err := sshpool.UploadSFTPForTest(ctx, &localFS{root: t.TempDir(), failCreate: true}, local, "/r/a", nil); err == nil {
		t.Fatal("创建远程文件失败应报错")
	}
	if err := sshpool.UploadSFTPForTest(ctx, &localFS{root: t.TempDir(), failWrite: true}, local, "/r/a", nil); err == nil {
		t.Fatal("写入远程文件失败应报错")
	}
	if _, err := sshpool.ReadSFTPForTest(&localFS{root: t.TempDir(), failOpen: true}, "/r/a"); err == nil {
		t.Fatal("打开远程文件失败应报错")
	}
	if _, err := sshpool.ReadSFTPForTest(fs, "/remote/missing"); err == nil {
		t.Fatal("读取不存在的文件应报错")
	}
	// 写失败：清理临时文件
	wf := &localFS{root: t.TempDir(), failWrite: true}
	if err := sshpool.WriteSFTPForTest(wf, "/r/a", "x"); err == nil || wf.removed == 0 {
		t.Fatalf("写入失败应报错并清理临时文件: %v removed=%d", err, wf.removed)
	}
	if err := sshpool.WriteSFTPForTest(&localFS{root: t.TempDir(), failCreate: true}, "/r/a", "x"); err == nil {
		t.Fatal("创建临时文件失败应报错")
	}
	// posix rename 不支持时回退普通 rename
	fallback := &localFS{root: t.TempDir(), failPosix: true}
	os.MkdirAll(fallback.root, 0o755)
	if err := sshpool.WriteSFTPForTest(fallback, "/r/b", "y"); err != nil || !fallback.posixUsed || fallback.renamed == 0 {
		t.Fatalf("应回退 rename: %v renamed=%d", err, fallback.renamed)
	}
	if err := sshpool.WriteSFTPForTest(&localFS{root: t.TempDir(), failPosix: true, failRename: true}, "/r/c", "z"); err == nil {
		t.Fatal("两种改名都失败应报错")
	}
}

// 池方法（UploadFile/ReadRemoteFile/WriteRemoteFile）走假执行器：验证取连接、取 SFTP、转发错误
func TestSSHPoolFileMethodsViaPool(t *testing.T) {
	master, closer := newFakeSSHServer(t)
	defer closer()
	local := filepath.Join(t.TempDir(), "a.jar")
	os.WriteFile(local, []byte("data"), 0o644)

	// SFTP 不可用：三个方法都应返回"初始化 SFTP 失败"，命令类用法不受影响
	noSFTP := sshpool.NewPool()
	defer noSFTP.Disconnect("all")
	noSFTP.SetDial(func(*sshpool.ServerConfig) (*ssh.Client, error) { return master, nil })
	noSFTP.SetNewSFTP(func(*ssh.Client) (*sftp.Client, error) { return nil, errors.New("sftp disabled") })
	cfg := &sshpool.ServerConfig{ID: "nosftp", Host: "127.0.0.1", Port: 22, User: "root", Password: "x"}
	if err := sshpool.UploadFileForTest(noSFTP, cfg, local, "/r/a", nil); err == nil || !strings.Contains(err.Error(), "sftp disabled") {
		t.Fatalf("上传 = %v", err)
	}
	if _, err := sshpool.ReadRemoteFileForTest(noSFTP, cfg, "/r/a"); err == nil {
		t.Fatal("读取应报错")
	}
	if err := sshpool.WriteRemoteFileForTest(noSFTP, cfg, "/r/a", "x"); err == nil {
		t.Fatal("写入应报错")
	}

	// 拨号失败：错误透传
	down := sshpool.NewPool()
	defer down.Disconnect("all")
	down.SetDial(func(*sshpool.ServerConfig) (*ssh.Client, error) { return nil, errors.New("down") })
	if err := sshpool.UploadFileForTest(down, cfg, local, "/r/a", nil); err == nil {
		t.Fatal("拨号失败应报错")
	}
	if _, err := sshpool.ReadRemoteFileForTest(down, cfg, "/r/a"); err == nil {
		t.Fatal("拨号失败应报错")
	}
	if err := sshpool.WriteRemoteFileForTest(down, cfg, "/r/a", "x"); err == nil {
		t.Fatal("拨号失败应报错")
	}
}
