package tests

import (
	"build-to-deploy/internal/store"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newStoreRelease(id, app, tag string) *store.Release {
	return &store.Release{ID: id, App: app, Tag: tag, Image: "app/" + app + ":" + tag, Status: store.StatusSuccess, CreatedAt: time.Now()}
}

func TestStoreStoreAddGetUpdateList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "releases.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// 时间戳显式指定：Windows 时钟粒度较粗，依赖 time.Now() 会让排序断言变得不确定
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.Local)
	r1 := newStoreRelease("r1", "a", "v1")
	r1.CreatedAt = base
	if err := st.Add(r1); err != nil {
		t.Fatal(err)
	}
	r2 := newStoreRelease("r2", "b", "v2")
	r2.Status = store.StatusFailed
	r2.CreatedAt = base.Add(time.Second)
	if err := st.Add(r2); err != nil {
		t.Fatal(err)
	}
	// 同一 ID 再次 Add 会追加一条新记录（Add 不做去重，调用方用 Update 修改）
	dup := newStoreRelease("r1", "a", "v1-dup")
	dup.CreatedAt = base.Add(2 * time.Second)
	st.Add(dup)

	got, err := st.Get("r1")
	if err != nil || got.Tag != "v1" {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	if _, err := st.Get("nope"); err == nil {
		t.Fatal("不存在的记录应报错")
	}

	updated, err := st.Update("r1", func(r *store.Release) { r.Tag = "v1-fix" })
	if err != nil || updated.Tag != "v1-fix" {
		t.Fatalf("Update = %+v, %v", updated, err)
	}
	if _, err := st.Update("nope", func(r *store.Release) {}); err == nil {
		t.Fatal("更新不存在记录应报错")
	}

	list := st.List("", 0)
	if len(list) != 3 || list[0].ID != "r1" || list[0].Tag != "v1-dup" {
		t.Fatalf("列表应按时间倒序（r1 被重新 Add 过）: %d 条, 首条 %s/%s", len(list), list[0].ID, list[0].Tag)
	}
	if list := st.List("a", 1); len(list) != 1 || list[0].App != "a" {
		t.Fatalf("按应用过滤/限条数失败: %+v", list)
	}

	// 重新打开：记录已落盘，运行中记录被标记为失败
	running := newStoreRelease("r3", "a", "v3")
	running.Status = store.StatusRunning
	st.Add(running)
	st2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = st2.Get("r3")
	if got.Status != store.StatusFailed {
		t.Fatalf("未完成记录应标记为 failed: %s", got.Status)
	}
}

func TestStoreStoreFindByImageAndFingerprint(t *testing.T) {
	st, _ := store.Open(filepath.Join(t.TempDir(), "r.json"))
	a := newStoreRelease("a1", "app", "v1")
	a.Image, a.ImageSummary, a.ImageFP = "sha256:aaa", "sha256:aaa", "fp-1"
	b := newStoreRelease("b1", "app", "v2")
	b.Image, b.ImageSummary, b.ImageFP = "sha256:bbb", "sha256:bbb", "fp-2"
	c := newStoreRelease("c1", "app", "v3")
	c.Status = store.StatusFailed
	c.ImageSummary, c.ImageFP = "sha256:ccc", "fp-3"
	for _, r := range []*store.Release{a, b, c} {
		st.Add(r)
	}
	if got := st.FindByImage("app", "sha256:aaa"); len(got) != 1 || got[0].ID != "a1" {
		t.Fatalf("FindByImage = %+v", got)
	}
	if got := st.FindByImage("app", "sha256:ccc"); len(got) != 0 {
		t.Fatalf("失败记录不应命中: %+v", got)
	}
	if got := st.FindFingerprint("app", "fp-2"); got == nil || got.ID != "b1" {
		t.Fatalf("FindFingerprint = %+v", got)
	}
	if got := st.FindFingerprint("app", ""); got != nil {
		t.Fatal("空指纹不应命中")
	}
	// 自动回滚成功的记录也算"用过该镜像"
	d := newStoreRelease("d1", "app", "v4")
	d.Status = store.StatusRolledBack
	d.Image, d.ImageSummary, d.ImageFP = "sha256:ddd", "sha256:ddd", "fp-4"
	st.Add(d)
	if got := st.FindByImage("app", "sha256:ddd"); len(got) != 1 {
		t.Fatalf("rolledback 记录应命中: %+v", got)
	}

	// 按产物 SHA256 查找：与 Tag、文件名无关
	a.FileSHA256 = "file-1"
	st.Update("a1", func(r *store.Release) { r.FileSHA256 = "file-1" })
	if got := st.FindByFileSHA256("app", "file-1"); got == nil || got.ID != "a1" {
		t.Fatalf("FindByFileSHA256 = %+v", got)
	}
	if got := st.FindByFileSHA256("app", ""); got != nil {
		t.Fatal("空产物摘要不应命中")
	}
	if got := st.FindByFileSHA256("other", "file-1"); got != nil {
		t.Fatal("不同应用不应命中")
	}
	failed := newStoreRelease("f1", "app", "v5")
	failed.Status = store.StatusFailed
	failed.FileSHA256 = "file-failed"
	st.Add(failed)
	if got := st.FindByFileSHA256("app", "file-failed"); got != nil {
		t.Fatalf("失败记录不应命中: %+v", got)
	}
}

func TestStoreStoreCopyAndDone(t *testing.T) {
	r := newStoreRelease("x", "a", "v")
	r.Status = store.StatusRunning
	r.Stages = []store.Stage{{Name: "build", Status: store.StatusRunning}}
	cp := r.Clone()
	cp.Stages[0].Status = store.StatusSuccess
	if r.Stages[0].Status != store.StatusRunning {
		t.Fatal("Clone 必须深拷贝 Stages")
	}
	if r.Done() {
		t.Fatal("running 不算结束")
	}
	r.Status = store.StatusSuccess
	if !r.Done() {
		t.Fatal("success 应算结束")
	}
	_ = os.Getenv("")
}
