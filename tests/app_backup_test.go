package tests

import (
	"build-to-deploy/internal/api"
	"build-to-deploy/internal/config"
	"build-to-deploy/internal/store"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestConfigAppBackupPerService 按服务的配置备份：文件名带服务名+版本号，恢复只影响该服务
func TestConfigAppBackupPerService(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	cfg.LatestTagFunc = func(appID string) string {
		if appID == "order-service" {
			return "v1.2.3"
		}
		return ""
	}

	setDesc := func(v string) {
		t.Helper()
		app := cfg.FindApp("order-service")
		if app == nil {
			t.Fatal("默认应用不存在")
		}
		app.Description = v
		if err := cfg.UpdateApp(app); err != nil {
			t.Fatalf("保存应用失败: %v", err)
		}
	}

	setDesc("第一次修改")
	backups, err := cfg.AppBackups()
	if err != nil {
		t.Fatalf("读取按服务备份失败: %v", err)
	}
	if len(backups) != 1 {
		t.Fatalf("保存 1 次应有 1 份按服务备份，实际 %d", len(backups))
	}
	b := backups[0]
	if b.AppID != "order-service" || b.Tag != "v1.2.3" {
		t.Fatalf("备份元信息不正确: %+v", b)
	}
	if !strings.HasPrefix(b.Name, "order-service_v1.2.3_") || !strings.HasSuffix(b.Name, ".yaml") {
		t.Fatalf("备份文件名应包含服务名与版本号: %s", b.Name)
	}
	if b.AppName != "order-service" || b.Image != "harbor.example.com/app/order-service" {
		t.Fatalf("备份应记录服务名与镜像: %+v", b)
	}
	if b.Size <= 0 || b.CreatedAt.IsZero() {
		t.Fatalf("备份大小/时间不正确: %+v", b)
	}

	// 第二次保存：最新备份是"第一次修改"之前的版本
	setDesc("第二次修改")
	backups, _ = cfg.AppBackups()
	if len(backups) != 2 {
		t.Fatalf("保存 2 次应有 2 份备份，实际 %d", len(backups))
	}
	if _, err := cfg.RestoreApp(backups[0].Name); err != nil {
		t.Fatalf("按服务恢复失败: %v", err)
	}
	if got := cfg.FindApp("order-service"); got.Description != "第一次修改" {
		t.Fatalf("恢复后描述应为第一次修改，实际 %q", got.Description)
	}
	// 恢复前会把当前版本再存档一份
	if backups, _ := cfg.AppBackups(); len(backups) != 3 {
		t.Fatalf("恢复后应再产生一份备份，实际 %d", len(backups))
	}

	// 非法备份名必须拒绝（路径穿越、不存在、非 yaml）
	for _, bad := range []string{"", "../evil.yaml", "sub/x.yaml", "missing.yaml", "note.txt"} {
		if _, err := cfg.RestoreApp(bad); err == nil {
			t.Errorf("RestoreApp(%q) 应该报错", bad)
		}
	}
}

// TestConfigAppBackupKeepsDeletedApp 删除应用前留档，按服务恢复可把服务加回来
func TestConfigAppBackupKeepsDeletedApp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg, _ := config.Load(path)
	if err := cfg.DeleteApp("order-service"); err != nil {
		t.Fatalf("删除应用失败: %v", err)
	}
	if cfg.FindApp("order-service") != nil {
		t.Fatal("应用应已删除")
	}
	backups, err := cfg.AppBackups()
	if err != nil || len(backups) != 1 {
		t.Fatalf("删除前应留一份备份: %v %d", err, len(backups))
	}
	if _, err := cfg.RestoreApp(backups[0].Name); err != nil {
		t.Fatalf("恢复被删除的服务失败: %v", err)
	}
	if cfg.FindApp("order-service") == nil {
		t.Fatal("恢复后应用应重新存在")
	}
}

// TestConfigAppBackupRetention 每个服务最多保留 20 份备份
func TestConfigAppBackupRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg, _ := config.Load(path)
	for i := 0; i < 25; i++ {
		app := cfg.FindApp("order-service")
		app.Description = "v" + strings.Repeat("x", i+1)
		if err := cfg.UpdateApp(app); err != nil {
			t.Fatal(err)
		}
	}
	backups, _ := cfg.AppBackups()
	if len(backups) != 20 {
		t.Fatalf("每个服务应只保留 20 份，实际 %d", len(backups))
	}
}

// TestConfigAppBackupTagSanitized 版本号中的非法文件名字符会被替换
func TestConfigAppBackupTagSanitized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg, _ := config.Load(path)
	cfg.LatestTagFunc = func(string) string { return "v1/2 3" }
	app := cfg.FindApp("order-service")
	app.Description = "改一下"
	if err := cfg.UpdateApp(app); err != nil {
		t.Fatal(err)
	}
	backups, _ := cfg.AppBackups()
	if len(backups) != 1 {
		t.Fatalf("应有 1 份备份，实际 %d", len(backups))
	}
	if !strings.Contains(backups[0].Name, "v1-2-3") {
		t.Fatalf("版本号应被清洗成安全字符: %s", backups[0].Name)
	}
}

// TestAPIAppBackupEndpoints 按服务备份接口：列表 / 恢复 / 非法请求
func TestAPIAppBackupEndpoints(t *testing.T) {
	srv, cfg, _, _ := newTestServer(t)
	h := srv.Handler()

	// 保存一次 demo 应用，触发按服务备份
	appBody := `{"id":"demo","name":"demo","type":"jar","serverId":"s1","image":"app/demo","artifactDir":"/tmp/a","buildDir":"/tmp/b","k8sYamlDir":"/tmp/y","k8sYamlFile":"demo.yaml","namespace":"prod","deployment":"demo","container":"demo"}`
	if rec := do(t, h, "POST", "/api/apps", appBody, nil); rec.Code != 200 {
		t.Fatalf("保存应用 = %d %s", rec.Code, rec.Body.String())
	}

	var backups []config.AppBackup
	decode(t, do(t, h, "GET", "/api/config/app-backups", "", nil), &backups)
	if len(backups) == 0 || backups[0].AppID != "demo" {
		t.Fatalf("按服务备份列表 = %+v", backups)
	}
	if !strings.HasPrefix(backups[0].Name, "demo_") {
		t.Fatalf("备份文件名应以服务名开头: %s", backups[0].Name)
	}

	rec := do(t, h, "POST", "/api/config/app-backups/restore", `{"name":"`+backups[0].Name+`"}`, nil)
	if rec.Code != 200 {
		t.Fatalf("按服务恢复 = %d %s", rec.Code, rec.Body.String())
	}
	if cfg.FindApp("demo") == nil {
		t.Fatal("恢复后 demo 应存在")
	}

	if rec := do(t, h, "POST", "/api/config/app-backups/restore", `bad`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/config/app-backups/restore", `{"name":"../evil.yaml"}`, nil); rec.Code != 400 {
		t.Fatalf("路径穿越应 400，实际 %d", rec.Code)
	}
}

// TestAPIReleaseLogDownloadName 发布日志下载：服务端 Content-Disposition 必须带服务名与版本号
func TestAPIReleaseLogDownloadName(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()
	eng := api.NewServerForTest(srv).Engine()

	rel := &store.Release{
		ID:        "20260101120000-abcdef12",
		App:       "demo",
		Tag:       "v1.0.0",
		Image:     "app/demo:v1.0.0",
		Status:    store.StatusSuccess,
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.Local),
	}
	if err := eng.Store.Add(rel); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(eng.LogPath(rel.ID), []byte("line-1\nline-2\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	rec := do(t, h, "GET", "/api/releases/"+rel.ID+"/log", "", nil)
	if rec.Code != 200 {
		t.Fatalf("下载日志 = %d %s", rec.Code, rec.Body.String())
	}
	cd := rec.Header().Get("Content-Disposition")
	want := "demo_v1.0.0_20260101-120000.log"
	if !strings.Contains(cd, "attachment") || !strings.Contains(cd, want) {
		t.Fatalf("Content-Disposition = %q，应包含 %q", cd, want)
	}
	if !strings.Contains(rec.Body.String(), "line-2") {
		t.Fatalf("日志内容不正确: %q", rec.Body.String())
	}

	// 不存在的发布记录返回 404
	if rec := do(t, h, "GET", "/api/releases/20260101120000-deadbeef/log", "", nil); rec.Code != 404 {
		t.Fatalf("不存在的日志应 404，实际 %d", rec.Code)
	}
}
