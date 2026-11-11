package tests

import (
	"path/filepath"
	"testing"

	"build-to-deploy/internal/config"
)

// TestConfigBackupRollback 每次保存自动备份，恢复时再备份当前，保留上限 20 份
func TestConfigBackupRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	// 首次生成配置文件时没有上一版，不应产生备份
	if backups, _ := cfg.Backups(); len(backups) != 0 {
		t.Fatalf("首次生成不应有备份，实际 %d 份", len(backups))
	}

	setDesc := func(v string) {
		t.Helper()
		app := cfg.FindApp("order-service")
		if app == nil {
			t.Fatal("默认应用不存在")
		}
		app.Description = v
		if err := cfg.UpdateApp(app); err != nil {
			t.Fatalf("保存配置失败: %v", err)
		}
	}
	setDesc("v1")
	if backups, _ := cfg.Backups(); len(backups) != 1 {
		t.Fatalf("保存 1 次应有 1 份备份，实际 %d 份", len(backups))
	}
	setDesc("v2")

	// 非法备份名必须拒绝（路径穿越、不存在的占位）
	for _, bad := range []string{"../evil.yaml", "a.yaml", "", "20240101-120000.yaml.bak", "20240101-120000.yaml"} {
		if err := cfg.Restore(bad); err == nil {
			t.Errorf("Restore(%q) 应该报错", bad)
		}
	}
	if got := cfg.FindApp("order-service"); got.Description != "v2" {
		t.Fatalf("非法恢复不应改动配置，实际 %q", got.Description)
	}

	// 恢复最新备份（v1 存档）：配置回到 v1，且当前 v2 自动再备份
	backups, _ := cfg.Backups()
	if len(backups) != 2 {
		t.Fatalf("保存 2 次应有 2 份备份，实际 %d 份", len(backups))
	}
	if err := cfg.Restore(backups[0].Name); err != nil {
		t.Fatalf("恢复失败: %v", err)
	}
	if got := cfg.FindApp("order-service"); got.Description != "v1" {
		t.Fatalf("恢复后应为 v1，实际 %q", got.Description)
	}
	if backups, _ := cfg.Backups(); len(backups) != 3 {
		t.Fatalf("恢复时应自动备份当前，期望 3 份，实际 %d 份", len(backups))
	}

	// 保留上限：再保存 25 次，仍只保留 20 份
	for i := 0; i < 25; i++ {
		setDesc("x")
	}
	if backups, _ := cfg.Backups(); len(backups) != 20 {
		t.Fatalf("保留份数应为 20，实际 %d", len(backups))
	}
}
