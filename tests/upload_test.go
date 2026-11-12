package tests

import (
	"build-to-deploy/internal/upload"
	"bytes"
	"os"
	"strings"
	"testing"
	"time"
)

func TestUploadUploadManagerLifecycle(t *testing.T) {
	m, err := upload.NewManager(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	u, err := m.Create("app.jar", 10, "tester")
	if err != nil {
		t.Fatal(err)
	}
	if !upload.ValidFileName(u.FileName) || upload.ValidFileName("a b.jar") {
		t.Fatal("文件名白名单判断不正确")
	}

	// offset 必须顺序递增
	if _, err := m.WriteChunk(u.ID, 5, strings.NewReader("12345")); err == nil {
		t.Fatal("乱序 offset 应报错")
	}
	if n, err := m.WriteChunk(u.ID, 0, bytes.NewReader([]byte("12345"))); err != nil || n != 5 {
		t.Fatalf("写分片 = %d, %v", n, err)
	}
	// 超出声明大小
	if _, err := m.WriteChunk(u.ID, 5, bytes.NewReader(make([]byte, 20))); err == nil {
		t.Fatal("超长分片应报错")
	}
	if _, err := m.Complete(u.ID); err == nil {
		t.Fatal("未收全时完成应报错")
	}
	if n, err := m.WriteChunk(u.ID, 5, strings.NewReader("67890")); err != nil || n != 10 {
		t.Fatalf("续传 = %d, %v", n, err)
	}
	done, err := m.Complete(u.ID)
	if err != nil || !done.Done || len(done.SHA256) != 64 {
		t.Fatalf("完成失败: %v", err)
	}
	// 已完成后继续写分片应报错，重复完成返回同一任务
	if _, err := m.WriteChunk(u.ID, 10, strings.NewReader("x")); err == nil {
		t.Fatal("完成后继续写应报错")
	}
	if again, err := m.Complete(u.ID); err != nil || again.ID != u.ID {
		t.Fatalf("重复完成 = %v", err)
	}
	snap := done.Snapshot()
	if snap.SHA256 == "" || snap.ID != u.ID {
		t.Fatalf("快照不正确: %s %s", snap.ID, snap.SHA256)
	}
}

func TestUploadUploadManagerValidation(t *testing.T) {
	m, _ := upload.NewManager(t.TempDir(), 100)
	if _, err := m.Create("app.jar", 0, ""); err == nil {
		t.Fatal("大小为 0 应报错")
	}
	if _, err := m.Create("app.jar", 200, ""); err == nil {
		t.Fatal("超过上限应报错")
	}
	if _, err := m.Create("bad name.jar", 10, ""); err == nil {
		t.Fatal("非法文件名应报错")
	}
	if _, err := m.Get("missing"); err == nil {
		t.Fatal("不存在应返回错误")
	}
	if _, err := m.WriteChunk("missing", 0, strings.NewReader("x")); err == nil {
		t.Fatal("不存在任务写分片应报错")
	}
	// 文件名中的路径会被 Base 去掉，只剩安全文件名
	u, err := m.Create("../../etc/app.jar", 5, "")
	if err != nil || u.FileName != "app.jar" {
		t.Fatalf("路径应被剥离: %s, %v", u.FileName, err)
	}
}

func TestUploadUploadManagerCleanupAndMissingFile(t *testing.T) {
	dir := t.TempDir()
	m, _ := upload.NewManager(dir, 1<<20)
	u, _ := m.Create("app.jar", 100, "")
	// 分片文件被外部删除：写分片应返回 upload.ErrNotFound 并忘掉任务
	if err := os.Remove(u.Path); err != nil {
		t.Fatal(err)
	}
	if _, err := m.WriteChunk(u.ID, 0, strings.NewReader("x")); err != upload.ErrNotFound {
		t.Fatalf("分片文件缺失应返回 ErrNotFound，实际 %v", err)
	}
	if _, err := m.Get(u.ID); err == nil {
		t.Fatal("任务应被移除")
	}

	// 过期任务被清理
	old, _ := m.Create("old.jar", 4, "")
	time.Sleep(30 * time.Millisecond)
	m.Cleanup(time.Millisecond)
	if _, err := m.Get(old.ID); err == nil {
		t.Fatal("过期任务应被清理")
	}
}

func TestUploadNewIDFormat(t *testing.T) {
	id := upload.NewID()
	if len(id) != 14+1+8 || id[14] != '-' {
		t.Fatalf("ID 格式不对: %q", id)
	}
}
