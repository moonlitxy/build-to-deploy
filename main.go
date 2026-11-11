package main

import (
	"context"
	"flag"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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
	cfgPath := flag.String("config", "", "配置文件路径 (默认依次探测 config.yaml、configs/config.yaml、configs/config.dev.yaml)")
	flag.Parse()

	targetCfg := *cfgPath
	if targetCfg == "" {
		// 生产配置优先于演练配置；模板放在 configs/ 下，同时兼容旧版放在根目录的 config.yaml
		for _, candidate := range []string{"config.yaml", "configs/config.yaml", "configs/config.dev.yaml"} {
			if _, err := os.Stat(candidate); err == nil {
				targetCfg = candidate
				break
			}
		}
		if targetCfg == "" {
			targetCfg = "configs/config.yaml"
		}
	}

	cfg, err := config.Load(targetCfg)
	if err != nil {
		log.Fatalf("加载配置失败 [%s]: %v", targetCfg, err)
	}

	// 本服务无登录认证，能力等价于远程 root shell：仅允许监听本机回环地址，避免局域网任意主机操控发布
	if h, _, _ := net.SplitHostPort(cfg.Server.Listen); h != "" && h != "localhost" {
		if ip := net.ParseIP(h); ip == nil || !ip.IsLoopback() {
			log.Fatalf("安全限制：listen 地址必须为本机回环地址 (如 127.0.0.1:8080)，当前配置为 %q", cfg.Server.Listen)
		}
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

	// 定时清理超过 24 小时未使用完毕的上传任务与临时分片文件，防止磁盘被残留数据占满
	go func() {
		t := time.NewTicker(10 * time.Minute)
		defer t.Stop()
		for range t.C {
			ups.Cleanup(24 * time.Hour)
		}
	}()

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
	log.Printf("📄 当前使用配置: %s", targetCfg)
	log.Printf("👉 浏览器直接访问: http://%s (无需登录密码)", displayAddr(cfg.Server.Listen))
	if cfg.Server.DryRun {
		log.Printf("⚠ 当前为演练模式 (DryRun)，模拟构建与K8s操作")
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// displayAddr 把监听地址转换为用户可在浏览器打开的地址（":8080" → "localhost:8080"）
func displayAddr(listen string) string {
	if strings.HasPrefix(listen, ":") {
		return "localhost" + listen
	}
	return listen
}
