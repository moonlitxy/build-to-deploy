package tests

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"build-to-deploy/internal/upload"
)

// maxPendingInTest 与 upload.maxPending 保持一致（上限 32）
const maxPendingInTest = 32

// TestUploadPendingLimitConcurrent 并发创建不得突破未完成任务上限，完成后名额应释放
func TestUploadPendingLimitConcurrent(t *testing.T) {
	m, err := upload.NewManager(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("创建上传管理器失败: %v", err)
	}

	var wg sync.WaitGroup
	var okCount, failCount int64
	var mu sync.Mutex
	var created []*upload.Upload
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := m.Create("a.jar", 10, "u")
			if err != nil {
				atomic.AddInt64(&failCount, 1)
				return
			}
			atomic.AddInt64(&okCount, 1)
			mu.Lock()
			created = append(created, u)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if okCount > maxPendingInTest {
		t.Fatalf("并发创建突破了上限：成功 %d 个，上限 %d", okCount, maxPendingInTest)
	}
	if okCount+failCount != 100 {
		t.Fatalf("成功+失败应为 100，实际 %d+%d", okCount, failCount)
	}

	// 完成一个任务后应释放一个名额
	first := created[0]
	if _, err := m.WriteChunk(first.ID, 0, strings.NewReader("0123456789")); err != nil {
		t.Fatalf("写入分片失败: %v", err)
	}
	if _, err := m.Complete(first.ID); err != nil {
		t.Fatalf("完成任务失败: %v", err)
	}
	if _, err := m.Create("b.jar", 10, "u"); err != nil {
		t.Fatalf("完成后应能再创建任务: %v", err)
	}
}
