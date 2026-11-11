// Package upload 实现分片 + 断点续传上传。
package upload

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type Upload struct {
	ID        string    `json:"id"`
	FileName  string    `json:"fileName"`
	Size      int64     `json:"size"`
	Received  int64     `json:"received"`
	SHA256    string    `json:"sha256,omitempty"`
	Done      bool      `json:"done"`
	Owner     string    `json:"-"`
	Path      string    `json:"-"`
	CreatedAt time.Time `json:"createdAt"`
	mu        sync.Mutex
}

type Manager struct {
	dir     string
	maxSize int64
	mu      sync.Mutex
	m       map[string]*Upload
}

func NewManager(dir string, maxSize int64) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Manager{dir: dir, maxSize: maxSize, m: map[string]*Upload{}}, nil
}

func NewID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return time.Now().Format("20060102150405") + "-" + hex.EncodeToString(b)[:8]
}

// maxPending 同时保留的未完成上传任务上限，防止无限创建临时文件
const maxPending = 32

// ErrNotFound 上传任务不存在或已过期
var ErrNotFound = errors.New("上传任务不存在或已过期")

// FileNameRe 上传文件名白名单：文件名会被拼入远程 shell 命令，禁止空格、分号、$、引号等
var fileNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ValidFileName 判断文件名是否符合安全白名单
func ValidFileName(name string) bool {
	return fileNameRe.MatchString(name)
}

// ValidateFileName 校验文件名
func ValidateFileName(name string) error {
	if !fileNameRe.MatchString(name) {
		return fmt.Errorf("文件名为空或不合法 (对于中文等特殊字符请重命名文件): %q", name)
	}
	return nil
}

func (m *Manager) Create(fileName string, size int64, owner string) (*Upload, error) {
	if size <= 0 {
		return nil, errors.New("文件大小不合法")
	}
	if size > m.maxSize {
		return nil, fmt.Errorf("文件超过上限 %d MB", m.maxSize>>20)
	}
	name := filepath.Base(fileName)
	if err := ValidateFileName(name); err != nil {
		return nil, err
	}
	m.mu.Lock()
	pending := 0
	for _, u := range m.m {
		if !u.Done {
			pending++
		}
	}
	if pending >= maxPending {
		m.mu.Unlock()
		return nil, errors.New("未完成的上传任务过多，请稍后重试")
	}
	m.mu.Unlock()
	u := &Upload{ID: NewID(), FileName: name, Size: size, Owner: owner, CreatedAt: time.Now()}
	u.Path = filepath.Join(m.dir, u.ID+".part")
	f, err := os.Create(u.Path)
	if err != nil {
		return nil, err
	}
	f.Close()
	m.mu.Lock()
	m.m[u.ID] = u
	m.mu.Unlock()
	return u, nil
}

func (m *Manager) Get(id string) (*Upload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.m[id]
	if !ok {
		return nil, ErrNotFound
	}
	return u, nil
}

// Snapshot 返回可安全序列化的副本。
func (u *Upload) Snapshot() Upload {
	u.mu.Lock()
	defer u.mu.Unlock()
	return Upload{ID: u.ID, FileName: u.FileName, Size: u.Size, Received: u.Received, SHA256: u.SHA256, Done: u.Done, CreatedAt: u.CreatedAt}
}

// WriteChunk 顺序写入分片，offset 必须等于已接收字节数（断点续传时客户端先查询 received）。
func (m *Manager) WriteChunk(id string, offset int64, r io.Reader) (int64, error) {
	u, err := m.Get(id)
	if err != nil {
		return 0, err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.Done {
		return u.Received, errors.New("上传已完成")
	}
	if offset != u.Received {
		return u.Received, fmt.Errorf("offset 不匹配，服务端已接收 %d", u.Received)
	}
	f, err := os.OpenFile(u.Path, os.O_WRONLY, 0o644)
	if err != nil {
		return u.Received, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return u.Received, err
	}
	n, err := io.Copy(f, io.LimitReader(r, u.Size-offset+1))
	if offset+n > u.Size {
		_ = f.Truncate(offset)
		return u.Received, errors.New("数据超过声明的文件大小")
	}
	if err != nil {
		// 只保留已完整写入的部分
		_ = f.Truncate(offset)
		return u.Received, err
	}
	u.Received += n
	return u.Received, nil
}

func (m *Manager) Complete(id string) (*Upload, error) {
	u, err := m.Get(id)
	if err != nil {
		return nil, err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.Done {
		return u, nil
	}
	if u.Received != u.Size {
		return nil, fmt.Errorf("文件不完整：%d/%d", u.Received, u.Size)
	}
	f, err := os.Open(u.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	u.SHA256 = hex.EncodeToString(h.Sum(nil))
	u.Done = true
	return u, nil
}

// Take 取走已完成的上传文件，移动到 dst，并移除上传任务。
func (m *Manager) Take(id, dst string) (*Upload, error) {
	u, err := m.Get(id)
	if err != nil {
		return nil, err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.Done {
		return nil, errors.New("文件尚未上传完成")
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return nil, err
	}
	if err := os.Rename(u.Path, dst); err != nil {
		return nil, err
	}
	m.mu.Lock()
	delete(m.m, id)
	m.mu.Unlock()
	return u, nil
}

// Cleanup 删除超过 ttl 未使用的上传任务（跳过进行中的任务，避免删除正在写入的分片文件）。
func (m *Manager) Cleanup(ttl time.Duration) {
	m.mu.Lock()
	var stale []*Upload
	for id, u := range m.m {
		if time.Since(u.CreatedAt) > ttl {
			stale = append(stale, u)
			delete(m.m, id)
		}
	}
	m.mu.Unlock()
	for _, u := range stale {
		u.mu.Lock()
		_ = os.Remove(u.Path)
		u.mu.Unlock()
	}
}
