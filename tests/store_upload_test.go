package tests

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"build-to-deploy/internal/store"
	"build-to-deploy/internal/upload"
)

// TestStoreRoundTrip 发布记录的增查改与倒序分页
func TestStoreRoundTrip(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "releases.json"))
	if err != nil {
		t.Fatalf("打开 store 失败: %v", err)
	}
	older := &store.Release{ID: "r1", App: "app1", Status: store.StatusRunning, CreatedAt: time.Now().Add(-time.Hour)}
	newer := &store.Release{ID: "r2", App: "app1", Status: store.StatusRunning, CreatedAt: time.Now()}
	if err := st.Add(older); err != nil {
		t.Fatalf("添加记录失败: %v", err)
	}
	if err := st.Add(newer); err != nil {
		t.Fatalf("添加记录失败: %v", err)
	}

	got, err := st.Get("r1")
	if err != nil || got.ID != "r1" {
		t.Fatalf("Get(r1) 失败: %+v, %v", got, err)
	}
	// 返回的是副本：修改不应影响库内数据
	got.Status = store.StatusSuccess
	again, _ := st.Get("r1")
	if again.Status != store.StatusRunning {
		t.Fatalf("Get 应返回副本，库内数据被污染: %q", again.Status)
	}

	list := st.List("app1", 0)
	if len(list) != 2 || list[0].ID != "r2" || list[1].ID != "r1" {
		t.Fatalf("List 应按时间倒序，实际: %+v", list)
	}
	if limited := st.List("", 1); len(limited) != 1 {
		t.Fatalf("limit=1 应只返回 1 条，实际 %d 条", len(limited))
	}

	updated, err := st.Update("r1", func(r *store.Release) { r.Status = store.StatusSuccess })
	if err != nil || updated.Status != store.StatusSuccess {
		t.Fatalf("Update 失败: %+v, %v", updated, err)
	}
	if _, err := st.Get("nope"); err == nil {
		t.Fatal("未知记录应该报错")
	}
}

// TestUploadLifecycle 分片上传完整链路：未完成前 Complete 必须失败，完成后锁定写入
func TestUploadLifecycle(t *testing.T) {
	m, err := upload.NewManager(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("创建上传管理器失败: %v", err)
	}
	if _, err := m.Create("中文.jar", 10, "u"); err == nil {
		t.Error("中文文件名应该被拒绝")
	}
	if _, err := m.Create("big.jar", 1<<20+1, "u"); err == nil {
		t.Error("超限文件应该被拒绝")
	}

	data := "hello world"
	u, err := m.Create("app.jar", int64(len(data)), "u")
	if err != nil {
		t.Fatalf("创建上传失败: %v", err)
	}
	// offset 必须等于已接收字节数
	if _, err := m.WriteChunk(u.ID, 5, strings.NewReader("hello")); err == nil {
		t.Fatal("offset 不匹配应该报错")
	}
	n, err := m.WriteChunk(u.ID, 0, strings.NewReader(data))
	if err != nil || n != int64(len(data)) {
		t.Fatalf("写入分片失败: n=%d, err=%v", n, err)
	}

	// 不完整的任务禁止 Complete（发布流水线只接受 Done 任务）
	half, err := m.Create("b.jar", 10, "u")
	if err != nil {
		t.Fatalf("创建上传失败: %v", err)
	}
	if _, err := m.WriteChunk(half.ID, 0, strings.NewReader("12345")); err != nil {
		t.Fatalf("写入分片失败: %v", err)
	}
	if _, err := m.Complete(half.ID); err == nil {
		t.Fatal("不完整的上传应该 Complete 失败")
	}

	done, err := m.Complete(u.ID)
	if err != nil || !done.Done {
		t.Fatalf("Complete 失败: %+v, %v", done, err)
	}
	sum := sha256.Sum256([]byte(data))
	if done.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 不匹配: %q", done.SHA256)
	}
	// 完成后禁止继续写入
	if _, err := m.WriteChunk(u.ID, int64(len(data)), strings.NewReader("x")); err == nil {
		t.Fatal("已完成的上传应该拒绝写入")
	}
	if _, err := m.Get("nope"); err == nil {
		t.Fatal("未知上传任务应该报错")
	}
}
