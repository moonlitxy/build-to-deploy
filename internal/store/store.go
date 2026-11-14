// Package store 以 JSON 文件持久化发布记录（零依赖、便于备份查看）。
package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	StatusPending  = "pending"
	StatusRunning  = "running"
	StatusSuccess  = "success"
	StatusFailed   = "failed"
	StatusSkipped  = "skipped"
	StatusCanceled = "canceled"
	// StatusNeedsConfirm 镜像内容与线上/历史版本完全相同，流水线在部署前中止、等待用户确认是否仍然部署
	StatusNeedsConfirm = "needs_confirm"
	StatusRolledBack   = "rolledback" // 发布失败并已自动回滚
)

type Stage struct {
	Name       string     `json:"name"`
	Title      string     `json:"title"`
	Status     string     `json:"status"`
	StartedAt  *time.Time `json:"startedAt,omitempty"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
}

type Release struct {
	ID         string `json:"id"`
	App        string `json:"app"`
	Kind       string `json:"kind"` // deploy | rollback
	Tag        string `json:"tag"`
	Image      string `json:"image"` // 实际部署的镜像引用（优先 image@digest）
	FileName   string `json:"fileName,omitempty"`
	FileSize   int64  `json:"fileSize,omitempty"`
	FileSHA256 string `json:"fileSha256,omitempty"`
	Force      bool   `json:"force"` // 是否为覆盖已有版本
	Operator   string `json:"operator"`
	Remark     string `json:"remark"`
	PrevImage  string `json:"prevImage"`
	// 构建输入指纹：产物内容 + 构建上下文（Dockerfile 等），相同即代表镜像内容相同，与 Tag 无关
	ImageFP      string `json:"imageFp,omitempty"`
	ImageSummary string `json:"imageSummary,omitempty"` // 构建后镜像内容摘要（sha256:...），空表示未检测
	// DuplicateOf 本地产物 SHA256 与哪条已发布记录完全相同时记录其 ID（换 Tag / 换文件名也会被识别）
	DuplicateOf string `json:"duplicateOf,omitempty"`
	// TargetSummary 用户显式确认"镜像内容与线上相同仍要部署"时记录的镜像摘要；
	// 与本次构建摘要一致即表示重复检查已被放行，流水线不再中止
	TargetSummary string     `json:"targetSummary,omitempty"`
	Mode          string     `json:"mode,omitempty"` // 部署形态：k8s | docker
	RollbackOf    string     `json:"rollbackOf,omitempty"`
	Status        string     `json:"status"`
	Error         string     `json:"error,omitempty"`
	Stages        []Stage    `json:"stages"`
	CreatedAt     time.Time  `json:"createdAt"`
	FinishedAt    *time.Time `json:"finishedAt,omitempty"`
}

func (r *Release) Clone() *Release {
	c := *r
	c.Stages = append([]Stage(nil), r.Stages...)
	return &c
}

func (r *Release) Done() bool {
	return r.Status != StatusRunning && r.Status != StatusPending
}

var ErrNotFound = errors.New("记录不存在")

type Store struct {
	mu   sync.Mutex
	path string
	list []*Release
}

func Open(path string) (*Store, error) {
	s := &Store{path: path}
	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &s.list); err != nil {
			return nil, err
		}
	}
	// 服务重启时，把未完成的记录标记为失败
	now := time.Now()
	for _, r := range s.list {
		if !r.Done() {
			r.Status = StatusFailed
			r.Error = "服务重启，流水线被中断"
			r.FinishedAt = &now
			for i := range r.Stages {
				if r.Stages[i].Status == StatusRunning {
					r.Stages[i].Status = StatusFailed
				}
			}
		}
	}
	return s, s.saveLocked()
}

func (s *Store) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s.list, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *Store) Add(r *Release) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.list = append(s.list, r.Clone())
	return s.saveLocked()
}

// Update 在锁内修改记录并持久化，返回修改后的副本。
func (s *Store) Update(id string, fn func(r *Release)) (*Release, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.list {
		if r.ID == id {
			fn(r)
			return r.Clone(), s.saveLocked()
		}
	}
	return nil, ErrNotFound
}

func (s *Store) Get(id string) (*Release, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.list {
		if r.ID == id {
			return r.Clone(), nil
		}
	}
	return nil, ErrNotFound
}

// List 按时间倒序返回记录，app 为空表示全部。
func (s *Store) List(app string, limit int) []*Release {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Release
	for _, r := range s.list {
		if app == "" || r.App == app {
			out = append(out, r.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// FindByImage 查找使用过该镜像引用（Tag 或摘要）的成功发布记录，按时间倒序。
// 发布成功或部署未成功但已自动回滚的记录，都说明该镜像确实被线上使用过。
func (s *Store) FindByImage(app, image string) []*Release {
	var out []*Release
	for _, r := range s.List(app, 0) {
		if r.Image == image && (r.Status == StatusSuccess || r.Status == StatusRolledBack) {
			out = append(out, r)
		}
	}
	return out
}

// FindFingerprint 查找同一构建输入指纹的最近成功发布记录（用于判断"换了 Tag 其实是同一个镜像"）
func (s *Store) FindFingerprint(app, fp string) *Release {
	if fp == "" {
		return nil
	}
	for _, r := range s.List(app, 0) {
		if r.ImageFP == fp && (r.Status == StatusSuccess || r.Status == StatusRolledBack) {
			return r
		}
	}
	return nil
}

// FindByImageSummary 查找镜像内容摘要相同（sha256:...）的最近一次成功发布记录。
// 摘要相同即代表是同一个镜像：换 Tag、换产物文件名都改变不了这一点。
func (s *Store) FindByImageSummary(app, sum string) *Release {
	if sum == "" {
		return nil
	}
	for _, r := range s.List(app, 0) {
		if r.ImageSummary == sum && (r.Status == StatusSuccess || r.Status == StatusRolledBack) {
			return r
		}
	}
	return nil
}

// FindByFileSHA256 查找同一产物内容（SHA256）最近一次成功发布记录。
// 产物内容相同即代表本次构建会产出与它一致的镜像内容：换 Tag、换文件名都掩盖不了这一点。
func (s *Store) FindByFileSHA256(app, sum string) *Release {
	if sum == "" {
		return nil
	}
	for _, r := range s.List(app, 0) {
		if r.FileSHA256 == sum && (r.Status == StatusSuccess || r.Status == StatusRolledBack) {
			return r
		}
	}
	return nil
}
