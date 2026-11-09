package main

import (
	"context"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"build-to-deploy/internal/api"
	"build-to-deploy/internal/config"
	"build-to-deploy/internal/k8s"
	"build-to-deploy/internal/pipeline"
	"build-to-deploy/internal/sshpool"
	"build-to-deploy/internal/store"
	"build-to-deploy/internal/upload"
	"build-to-deploy/web"
)

var version = "2.0-local"

func main() {
	cfgPath := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	if abs, err := filepath.Abs(cfg.Server.DataDir); err == nil {
		cfg.Server.DataDir = abs
	}
	if err := os.MkdirAll(cfg.Server.DataDir, 0755); err != nil {
		log.Fatal(err)
	}

	st, err := store.Open(filepath.Join(cfg.Server.DataDir, "releases.json"))
	if err != nil {
		log.Fatalf("打开发布记录失败: %v", err)
	}

	ups, err := upload.NewManager(filepath.Join(cfg.Server.DataDir, "uploads"), cfg.Server.MaxUploadMB<<20)
	if err != nil {
		log.Fatal(err)
	}

	pool := sshpool.GlobalPool
	kubeCli := k8s.NewClient(pool, cfg.Server.DryRun)

	engine := pipeline.NewEngine(cfg, st, ups, pool, kubeCli, cfg.Server.DataDir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	static, err := fs.Sub(web.FS, "static")
	if err != nil {
		log.Fatal(err)
	}

	server := api.NewServer(cfg, engine, kubeCli, pool, static)

	srv := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("🚀 Build-to-Deploy 本地发布平台启动成功 [%s]", version)
	log.Printf("👉 浏览器直接访问: http://localhost%s (无需登录密码)", cfg.Server.Listen)
	if cfg.Server.DryRun {
		log.Printf("⚠ 当前为演练模式 (DryRun)，模拟构建与K8s操作")
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
