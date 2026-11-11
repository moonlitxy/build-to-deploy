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
	lastUsed  time.Time
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
	u := &Upload{ID: NewID(), FileName: name, Size: size, Owner: owner, CreatedAt: time.Now(), lastUsed: time.Now()}
	u.Path = filepath.Join(m.dir, u.ID+".part")
	f, err := os.Create(u.Path)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	f.Close()
	// 占位与计数在同一临界区完成，避免并发创建突破 maxPending
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
		// 分片文件已被清理：上传任务同样视为过期，既报 404 也让客户端重建任务
		if os.IsNotExist(err) {
			m.forget(id, u)
			return u.Received, ErrNotFound
		}
		return u.Received, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return u.Received, err
	}
	n, err := io.Copy(f, r)
	u.lastUsed = time.Now()
	if offset+n > u.Size || err != nil {
		// 先关闭再截断：未 Close 的文件描述符可能把缓冲中的越界数据在关闭时再次写出
		_ = f.Close()
		if offset+n > u.Size {
			// 超出的分片整段作废：文件只保留 offset 之前的完整内容，下次从同一 offset 重传
			_ = os.Truncate(u.Path, offset)
			return u.Received, errors.New("数据超过声明的文件大小")
		}
		// 只保留本次写入的完整前缀。截断到 offset+n 而不是 offset，
		// 否则客户端断线重试后，已收到的数据与服务端记录会对不上
		_ = os.Truncate(u.Path, offset+n)
		return u.Received, err
	}
	u.Received += n
	return u.Received, nil
}

// forget 从任务表移除已被清理的上传（仅在仍指向同一对象时）
func (m *Manager) forget(id string, u *Upload) {
	m.mu.Lock()
	if cur, ok := m.m[id]; ok && cur == u {
		delete(m.m, id)
	}
	m.mu.Unlock()
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

// Cleanup 删除超过 ttl 未使用的上传任务。
// 必须持有上传任务自身的锁才删除分片文件：大文件上传可能持续数小时，
// 仅凭创建时间判断会把正在写入的文件删掉，导致后续分片写进不存在的文件。
func (m *Manager) Cleanup(ttl time.Duration) {
	m.mu.Lock()
	var ids []string
	for id := range m.m {
		ids = append(ids, id)
	}
	m.mu.Unlock()

	for _, id := range ids {
		m.mu.Lock()
		u, ok := m.m[id]
		m.mu.Unlock()
		if !ok {
			continue
		}
		u.mu.Lock()
		if time.Since(u.lastUsed) > ttl {
			_ = os.Remove(u.Path)
			m.forget(id, u)
		}
		u.mu.Unlock()
	}
}
