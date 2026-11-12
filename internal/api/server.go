package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/dockerrun"
	"build-to-deploy/internal/k8s"
	"build-to-deploy/internal/pipeline"
	"build-to-deploy/internal/shellrun"
	"build-to-deploy/internal/sshpool"
	"build-to-deploy/internal/store"
	"build-to-deploy/internal/upload"
)

const maxChunkSize = 64 << 20

// maskedValue 密码脱敏占位符；前端回传该值表示"不修改密码"
const maskedValue = "******"

// maxListLimit 发布记录列表最大返回条数
const maxListLimit = 500

type Server struct {
	cfg    *config.Config
	engine *pipeline.Engine
	kube   *k8s.Client
	pool   *sshpool.Pool
	run    shellrun.Runner
	static fs.FS

	// pathPickerBusy 系统文件选择框的全局占用标记：GUI 里同时只会有一个模态对话框，
	// 并发请求直接返回 409，避免排队中的请求在用户关掉一个后再弹出来
	pathPickerMu   sync.Mutex
	pathPickerBusy bool
}

func NewServer(cfg *config.Config, engine *pipeline.Engine, kube *k8s.Client, pool *sshpool.Pool, static fs.FS) *Server {
	return &Server{
		cfg:    cfg,
		engine: engine,
		kube:   kube,
		pool:   pool,
		run:    pool,
		static: static,
	}
}

// WithRunner 替换远程命令执行实现（测试注入假 SSH 使用），返回自身便于链式调用
func (s *Server) WithRunner(r shellrun.Runner) *Server {
	s.run = r
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// 服务器管理 & SSH 连接测试（长连接心跳池）
	mux.HandleFunc("GET /api/servers", s.listServers)
	mux.HandleFunc("POST /api/servers", s.saveServer)
	mux.HandleFunc("DELETE /api/servers/{id}", s.deleteServer)
	mux.HandleFunc("POST /api/servers/test", s.testServer)
	mux.HandleFunc("POST /api/servers/disconnect", s.disconnectServer)
	mux.HandleFunc("GET /api/servers/connections", s.activeConnections)

	// 应用管理（支持产物目录、构建目录、K8s目录独立配置）
	mux.HandleFunc("GET /api/apps", s.listApps)
	mux.HandleFunc("POST /api/apps", s.saveApp)
	mux.HandleFunc("DELETE /api/apps/{id}", s.deleteApp)
	mux.HandleFunc("GET /api/apps/{id}/local-artifact", s.inspectLocalArtifact)
	mux.HandleFunc("POST /api/local/pick-path", s.pickLocalPath)

	// 配置备份与回滚（每次保存自动备份，改错可恢复）
	mux.HandleFunc("GET /api/config/backups", s.listConfigBackups)
	mux.HandleFunc("POST /api/config/restore", s.restoreConfig)
	// 按服务的配置备份：文件名带服务名与版本号，恢复只影响该服务
	mux.HandleFunc("GET /api/config/app-backups", s.listAppBackups)
	mux.HandleFunc("POST /api/config/app-backups/restore", s.restoreAppBackup)

	// 预检版本冲突（满足需求 2）
	mux.HandleFunc("POST /api/precheck", s.precheck)
	mux.HandleFunc("POST /api/artifacts/inspect", s.inspectArtifact)
	mux.HandleFunc("GET /api/deploy-target", s.containerStatus)

	// K8s 命名空间与 Pod 状态监控（满足需求 3）
	mux.HandleFunc("GET /api/k8s/namespaces", s.getNamespaces)
	mux.HandleFunc("GET /api/k8s/pods", s.getPods)
	mux.HandleFunc("GET /api/k8s/pod-logs", s.getPodLogs)
	mux.HandleFunc("POST /api/k8s/restart", s.restartDeployment)

	// 本地文件分片上传
	mux.HandleFunc("POST /api/uploads", s.createUpload)
	mux.HandleFunc("GET /api/uploads/{id}", s.getUpload)
	mux.HandleFunc("PUT /api/uploads/{id}", s.writeChunk)
	mux.HandleFunc("POST /api/uploads/{id}/complete", s.completeUpload)

	// 发布流水线
	mux.HandleFunc("GET /api/releases", s.listReleases)
	mux.HandleFunc("POST /api/releases", s.startRelease)
	mux.HandleFunc("POST /api/releases/{id}/rollback", s.rollbackRelease)
	mux.HandleFunc("GET /api/releases/{id}", s.getRelease)
	mux.HandleFunc("GET /api/releases/{id}/events", s.releaseEvents)
	mux.HandleFunc("GET /api/releases/{id}/log", s.releaseLog)
	mux.HandleFunc("POST /api/releases/{id}/cancel", s.cancelRelease)

	// 前端页面托管（无需账号密码，直接本地访问）
	mux.Handle("/", http.FileServer(http.FS(s.static)))

	return logRequests(guard(mux))
}

// maskServer 返回脱敏后的服务器配置：接口不回传 SSH 密码/口令
func maskServer(srv *sshpool.ServerConfig) *sshpool.ServerConfig {
	cp := *srv
	if cp.Password != "" {
		cp.Password = maskedValue
	}
	if cp.Passphrase != "" {
		cp.Passphrase = maskedValue
	}
	return &cp
}

// guard 本服务无登录认证，且能力等价于远程 root shell，必须保证只有本机页面能调用：
// 1) Host 头限定为本机地址，防止恶意网页通过 DNS rebinding 访问
// 2) 写操作要求 Origin 为空（curl/脚本）或同源，防止任意网页 CSRF 触发发布
func guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLocalHost(r.Host) {
			writeErr(w, http.StatusForbidden, "仅允许通过 localhost 访问本服务: "+r.Host)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			if origin := r.Header.Get("Origin"); origin != "" && !sameOrigin(origin, r.Host) {
				writeErr(w, http.StatusForbidden, "跨站请求已被拒绝 (Origin 不匹配)")
				return
			}
			// 跨站表单提交（text/plain 等"简单" Content-Type）不触发预检、也可能不带 Origin，
			// 因此除上传分片外，写接口一律只接受 JSON
			if ct := strings.ToLower(r.Header.Get("Content-Type")); ct != "" && !isJSONContentType(ct) && !(r.Method == http.MethodPut && strings.HasPrefix(ct, "application/octet-stream")) {
				writeErr(w, http.StatusUnsupportedMediaType, "仅接受 application/json 请求体")
				return
			}
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func isJSONContentType(ct string) bool {
	// 允许 "application/json; charset=utf-8" 这类带参数的形式
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return ct == "application/json"
}

func isLocalHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func sameOrigin(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Host, host)
}

func logRequests(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h.ServeHTTP(w, r)
		if strings.HasPrefix(r.URL.Path, "/api/") && r.Method != http.MethodPut && !strings.Contains(r.URL.Path, "/events") {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
		}
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}

// hostRe SSH 主机名 / 用户名白名单：不允许空白、引号、分号等可进入 shell 或显示信息的字符
var hostRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@%\[\]-]{0,253}$`)

// checkID 校验会参与文件路径拼接的 ID（发布 ID、上传 ID、应用 ID）
func checkID(id string) error {
	if !config.IDRe.MatchString(id) {
		return fmt.Errorf("ID 格式不合法: %q", id)
	}
	return nil
}

// releaseID 读取并校验路径中的发布 ID，失败时已写出响应
func releaseID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if err := checkID(id); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return "", false
	}
	return id, true
}

// checkK8sName 校验 K8s 资源名格式：用户输入问题返回 400，避免落到 k8s 层变成 500
func checkK8sName(w http.ResponseWriter, kind, v string) bool {
	if err := k8s.CheckName(kind, v); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

func readJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 2<<20))
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("请求格式错误: %w", err)
	}
	return nil
}

// ---------- 服务器 SSH 管理 ----------

func (s *Server) listServers(w http.ResponseWriter, r *http.Request) {
	list := s.cfg.ListServers()
	out := make([]*sshpool.ServerConfig, 0, len(list))
	for _, srv := range list {
		out = append(out, maskServer(srv))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) saveServer(w http.ResponseWriter, r *http.Request) {
	var srv sshpool.ServerConfig
	if err := readJSON(r, &srv); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if srv.ID == "" {
		srv.ID = upload.NewID()
	}
	if srv.Port <= 0 {
		srv.Port = 22
	}
	if srv.Host == "" || srv.User == "" {
		writeErr(w, http.StatusBadRequest, "主机地址和用户名不能为空")
		return
	}
	if srv.Port < 1 || srv.Port > 65535 {
		writeErr(w, http.StatusBadRequest, "端口必须在 1-65535 之间")
		return
	}
	// 主机、用户会被拼进 ssh command 的显示信息，认证方式决定是否会送出新添加的服务器本机凭据
	if !hostRe.MatchString(srv.Host) {
		writeErr(w, http.StatusBadRequest, "主机地址不合法（只允许域名、IPv4 或 IPv6 字面量）")
		return
	}
	if !hostRe.MatchString(srv.User) {
		writeErr(w, http.StatusBadRequest, "用户名不合法")
		return
	}
	if srv.AuthType != "" && srv.AuthType != "password" && srv.AuthType != "key" && srv.AuthType != "agent" {
		writeErr(w, http.StatusBadRequest, "认证方式只能是 password、key 或 agent")
		return
	}
	// 编辑时前端回填的是脱敏占位符（或留空），收到时保留原凭据，避免把真实密码覆盖成 ******/空
	if old := s.cfg.FindServer(srv.ID); old != nil && old.ID == srv.ID {
		if (srv.Password == maskedValue || srv.Password == "") && old.Password != "" {
			srv.Password = old.Password
		}
		if srv.Passphrase == maskedValue && old.Passphrase != "" {
			srv.Passphrase = old.Passphrase
		}
	}
	if err := s.cfg.UpdateServer(&srv); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	// 连接池按服务器 ID 缓存：host / 端口 / 凭据变更后必须失效旧长连接，否则继续复用 stale 连接
	if s.pool != nil {
		s.pool.Invalidate(&srv)
	}
	writeJSON(w, http.StatusOK, maskServer(&srv))
}

func (s *Server) deleteServer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := checkID(id); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.cfg.DeleteServer(id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	// 删除配置的同时断开其长连接，避免残留连接继续占用远端会话（上面已断开一次，这里兜底处理并发新建）
	if s.pool != nil {
		s.pool.Disconnect(id)
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) testServer(w http.ResponseWriter, r *http.Request) {
	var srv sshpool.ServerConfig
	if err := readJSON(r, &srv); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// 页面上回传的是脱敏占位符（或用户只改了部分字段）：按 ID 取已保存的真实凭据补全。
	// 之前只在下发 host 为空时才回退，导致连接按钮把 "******" 当密码发送，认证必然失败。
	if srv.ID != "" {
		if existing := s.cfg.FindServer(srv.ID); existing != nil {
			if srv.Host == "" {
				srv.Host = existing.Host
			}
			if srv.User == "" {
				srv.User = existing.User
			}
			if srv.Port <= 0 {
				srv.Port = existing.Port
			}
			if srv.AuthType == "" {
				srv.AuthType = existing.AuthType
			}
			if srv.KeyPath == "" {
				srv.KeyPath = existing.KeyPath
			}
			// 用户显式输入的新密码优先；留空或仍是掩码时使用已保存值
			if srv.Password == "" || srv.Password == maskedValue {
				srv.Password = existing.Password
			}
			if srv.Passphrase == "" || srv.Passphrase == maskedValue {
				srv.Passphrase = existing.Passphrase
			}
		}
	}
	if s.pool == nil {
		writeErr(w, http.StatusInternalServerError, "SSH 连接池未初始化")
		return
	}
	if err := s.pool.TestConnection(&srv); err != nil {
		writeErr(w, http.StatusBadRequest, "SSH 连接失败: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": fmt.Sprintf("SSH 连接成功 [%s@%s]！心跳长连接已保持。", srv.User, srv.Host)})
}

func (s *Server) disconnectServer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ServerID string `json:"serverId"`
	}
	_ = readJSON(r, &req)
	n := 0
	if s.pool != nil {
		n = s.pool.Disconnect(req.ServerID)
	}
	msg := fmt.Sprintf("已断开 %d 个 SSH 长连接并释放本地资源", n)
	if n == 0 {
		msg = "当前没有活动的长连接"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"closed":  n,
		"message": msg,
	})
}

func (s *Server) activeConnections(w http.ResponseWriter, r *http.Request) {
	var keys []string
	if s.pool != nil {
		keys = s.pool.ActiveKeys()
	}
	var details []map[string]string
	for _, k := range keys {
		if srv := s.cfg.FindServer(k); srv != nil && srv.ID == k {
			details = append(details, map[string]string{
				"id":   srv.ID,
				"name": srv.Name,
				"host": srv.Host,
				"user": srv.User,
			})
		} else {
			details = append(details, map[string]string{
				"id":   k,
				"name": k,
				"host": k,
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active":  keys,
		"servers": details,
		"count":   len(keys),
	})
}

// ---------- 应用配置管理 ----------

func (s *Server) listApps(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.cfg.ListApps())
}

func (s *Server) saveApp(w http.ResponseWriter, r *http.Request) {
	var app config.AppConfig
	if err := readJSON(r, &app); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if app.ID == "" {
		app.ID = app.Name
	}
	if app.Name == "" {
		writeErr(w, http.StatusBadRequest, "应用名称不能为空")
		return
	}
	// 校验不通过属用户输入问题，返回 400 而非 500
	if err := config.ValidateApp(&app); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.cfg.UpdateApp(&app); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, app)
}

func (s *Server) deleteApp(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := checkID(id); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.cfg.DeleteApp(id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// ---------- 配置备份与回滚 ----------

func (s *Server) listConfigBackups(w http.ResponseWriter, r *http.Request) {
	backups, err := s.cfg.Backups()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if backups == nil {
		backups = []config.ConfigBackup{}
	}
	writeJSON(w, http.StatusOK, backups)
}

func (s *Server) listAppBackups(w http.ResponseWriter, r *http.Request) {
	backups, err := s.cfg.AppBackups()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if backups == nil {
		backups = []config.AppBackup{}
	}
	writeJSON(w, http.StatusOK, backups)
}

func (s *Server) restoreAppBackup(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	app, err := s.cfg.RestoreApp(req.Name)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// 只恢复单个服务，服务器凭据未变：无需断开长连接
	writeJSON(w, http.StatusOK, app)
}

func (s *Server) restoreConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.cfg.Restore(req.Name); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// 服务器配置可能已回退到旧凭据，断开全部长连接，下次使用时重建
	if s.pool != nil {
		s.pool.Disconnect("all")
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) inspectLocalArtifact(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := checkID(id); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	app := s.cfg.FindApp(id)
	if app == nil {
		writeErr(w, http.StatusNotFound, "应用不存在")
		return
	}
	resolved := app.ResolveLocalArtifactPath()
	hasLocalBuild := strings.TrimSpace(app.LocalBuildCmd) != "" && resolved != ""
	res := map[string]any{
		"configured":    resolved != "",
		"hasLocalBuild": hasLocalBuild,
		"projectDir":    app.ResolveLocalProjectDir(),
		"buildCmd":      app.LocalBuildCmd,
		"path":          resolved,
		"exists":        false,
	}
	if resolved != "" {
		if st, err := os.Stat(resolved); err == nil && !st.IsDir() {
			res["exists"] = true
			res["fileName"] = st.Name()
			res["size"] = st.Size()
			res["modTime"] = st.ModTime()
		}
	}
	writeJSON(w, http.StatusOK, res)
}

// pickLocalPath 唤起本机系统原生的文件夹或文件选择对话框，返回真实绝对路径
func (s *Server) pickLocalPath(w http.ResponseWriter, r *http.Request) {
	s.pathPickerMu.Lock()
	if s.pathPickerBusy {
		s.pathPickerMu.Unlock()
		writeErr(w, http.StatusConflict, "已有文件选择框正在打开，请先关闭它")
		return
	}
	s.pathPickerBusy = true
	s.pathPickerMu.Unlock()
	defer func() {
		s.pathPickerMu.Lock()
		s.pathPickerBusy = false
		s.pathPickerMu.Unlock()
	}()

	var req struct {
		Mode   string `json:"mode"` // "dir" 或 "file"
		Prompt string `json:"prompt"`
	}
	_ = readJSON(r, &req)
	isDir := req.Mode == "dir"
	prompt := sanitizePrompt(req.Prompt)
	if prompt == "" {
		if isDir {
			prompt = "请选择本地项目源码目录"
		} else {
			prompt = "请选择本地构建产物文件"
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		script := fmt.Sprintf(`POSIX path of (choose file with prompt "%s")`, prompt)
		if isDir {
			script = fmt.Sprintf(`POSIX path of (choose folder with prompt "%s")`, prompt)
		}
		cmd = exec.CommandContext(ctx, "osascript", "-e", script)
	case "windows":
		var ps string
		if isDir {
			ps = `Add-Type -AssemblyName System.Windows.Forms; $f = New-Object System.Windows.Forms.FolderBrowserDialog; if ($f.ShowDialog() -eq 'OK') { Write-Output $f.SelectedPath }`
		} else {
			ps = `Add-Type -AssemblyName System.Windows.Forms; $f = New-Object System.Windows.Forms.OpenFileDialog; if ($f.ShowDialog() -eq 'OK') { Write-Output $f.FileName }`
		}
		cmd = exec.CommandContext(ctx, "powershell", "-NoProfile", "-Command", ps)
	default:
		if isDir {
			cmd = exec.CommandContext(ctx, "zenity", "--file-selection", "--directory", "--title="+prompt)
		} else {
			cmd = exec.CommandContext(ctx, "zenity", "--file-selection", "--title="+prompt)
		}
	}

	out, err := cmd.Output()
	if err != nil {
		// 用户点击了取消
		writeJSON(w, http.StatusOK, map[string]any{"canceled": true, "path": ""})
		return
	}
	selected := strings.TrimSpace(string(out))
	if isDir && len(selected) > 1 {
		selected = strings.TrimRight(selected, "/")
	}
	writeJSON(w, http.StatusOK, map[string]any{"canceled": false, "path": selected})
}

// ---------- 部署形态：K8s / Docker 容器 ----------

// containerStatus 查询宿主机上同名容器是否在运行（只读，供页面展示"当前形态"）
func (s *Server) containerStatus(w http.ResponseWriter, r *http.Request) {
	appID := r.URL.Query().Get("appId")
	app := s.cfg.FindApp(appID)
	if app == nil {
		writeErr(w, http.StatusNotFound, "应用不存在")
		return
	}
	srv := s.cfg.FindServer(app.ServerID)
	if srv == nil {
		writeErr(w, http.StatusBadRequest, "应用未绑定有效的 SSH 服务器")
		return
	}
	out := map[string]any{"appId": app.ID, "deployMode": app.DeployMode}
	if app.DeployMode != config.DeployModeDocker || strings.TrimSpace(app.DockerContainer) == "" {
		writeJSON(w, http.StatusOK, out)
		return
	}
	out["container"] = app.DockerContainer
	insp, err := dockerrun.InspectCmdFor(app)
	if err != nil {
		writeJSON(w, http.StatusOK, out)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	raw, err := s.run.ExecuteOutput(ctx, srv, insp)
	if err != nil {
		out["error"] = err.Error()
		writeJSON(w, http.StatusOK, out)
		return
	}
	status, image := dockerrun.ParseInspect(raw)
	out["status"] = status
	out["image"] = image
	out["running"] = status == "running"
	writeJSON(w, http.StatusOK, out)
}

// ---------- 构建输入指纹预检（上传前判断新 Tag 是否是同一个镜像） ----------

func (s *Server) inspectArtifact(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AppID             string `json:"appId"`
		Tag               string `json:"tag"`
		UploadID          string `json:"uploadId"`
		UseLocalArtifact  bool   `json:"useLocalArtifact"`
		LocalArtifactPath string `json:"localArtifactPath"`
		BuildDir          string `json:"buildDir"`
		BuildCmd          string `json:"buildCmd"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	app := s.cfg.FindApp(req.AppID)
	if app == nil {
		writeErr(w, http.StatusNotFound, "应用不存在")
		return
	}
	// 应用卡片上尚未落盘的编辑（修改构建配置弹窗里的构建目录/构建命令）优先于已保存配置
	app = app.WithOverrides(req.BuildDir, req.BuildCmd)
	src := pipeline.ArtifactSource{Tag: strings.TrimSpace(req.Tag), UploadID: req.UploadID}
	if src.UploadID == "" {
		path := app.ResolveLocalArtifactPath()
		if p := strings.TrimSpace(req.LocalArtifactPath); p != "" {
			path = config.ExpandHome(p)
		}
		if path == "" {
			writeJSON(w, http.StatusOK, map[string]any{"fingerprint": ""})
			return
		}
		src.LocalArtifact = path
	}
	fp, err := s.engine.InspectArtifact(app, src)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	out := map[string]any{
		"fingerprint": fp.Fingerprint,
		"sha256":      fp.SHA256,
		"tagFree":     fp.TagFree,
		"skipped":     fp.Skipped,
	}
	if fp.Fingerprint == "" {
		out["reason"] = s.engine.FingerprintSkipReason(app, src)
	}
	if prev := s.engine.Store.FindFingerprint(app.ID, fp.Fingerprint); prev != nil {
		out["previous"] = map[string]any{"tag": prev.Tag, "image": prev.Image, "createdAt": prev.CreatedAt}
	}
	writeJSON(w, http.StatusOK, out)
}

// ---------- 预检版本冲突 (需求 2) ----------

func (s *Server) precheck(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AppID string `json:"appId"`
		Tag   string `json:"tag"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	app := s.cfg.FindApp(req.AppID)
	if app == nil {
		writeErr(w, http.StatusNotFound, "应用不存在")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	res, err := s.engine.Precheck(ctx, app, strings.TrimSpace(req.Tag))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// ---------- K8s 命名空间与 Pod 状态监控 (需求 3) ----------

func (s *Server) getNamespaces(w http.ResponseWriter, r *http.Request) {
	serverId := r.URL.Query().Get("serverId")
	srv := s.cfg.FindServer(serverId)
	if srv == nil {
		writeErr(w, http.StatusBadRequest, "未指定或找不到 SSH 服务器")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	nsList, err := s.kube.ListNamespaces(ctx, srv)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, nsList)
}

func (s *Server) getPods(w http.ResponseWriter, r *http.Request) {
	serverId := r.URL.Query().Get("serverId")
	ns := r.URL.Query().Get("namespace")
	srv := s.cfg.FindServer(serverId)
	if srv == nil {
		writeErr(w, http.StatusBadRequest, "未指定或找不到 SSH 服务器")
		return
	}
	// 空命名空间由 k8s 层默认填充为 default；"*" 表示查看全部命名空间（前端「全部命名空间」选项）
	if ns == "" {
		ns = "default"
	}
	if ns != k8s.AllNamespaces && !checkK8sName(w, "命名空间", ns) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	pods, err := s.kube.ListPods(ctx, srv, ns)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, pods)
}

func (s *Server) getPodLogs(w http.ResponseWriter, r *http.Request) {
	serverId := r.URL.Query().Get("serverId")
	ns := r.URL.Query().Get("namespace")
	pod := r.URL.Query().Get("pod")
	container := r.URL.Query().Get("container")
	tail, _ := strconv.Atoi(r.URL.Query().Get("tail"))

	srv := s.cfg.FindServer(serverId)
	if srv == nil {
		writeErr(w, http.StatusBadRequest, "未指定或找不到 SSH 服务器")
		return
	}
	if ns != "" && !checkK8sName(w, "命名空间", ns) {
		return
	}
	if !checkK8sName(w, "Pod", pod) {
		return
	}
	if container != "" && !checkK8sName(w, "容器", container) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	logs, err := s.kube.GetPodLogs(ctx, srv, ns, pod, container, tail)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"logs": logs})
}

func (s *Server) restartDeployment(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ServerID   string `json:"serverId"`
		Namespace  string `json:"namespace"`
		Deployment string `json:"deployment"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	srv := s.cfg.FindServer(req.ServerID)
	if srv == nil {
		writeErr(w, http.StatusBadRequest, "未指定或找不到 SSH 服务器")
		return
	}
	if req.Deployment == "" {
		writeErr(w, http.StatusBadRequest, "Deployment 名称不能为空")
		return
	}
	if !checkK8sName(w, "Deployment", req.Deployment) {
		return
	}
	if req.Namespace != "" && !checkK8sName(w, "命名空间", req.Namespace) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := s.kube.RestartDeployment(ctx, srv, req.Namespace, req.Deployment); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": fmt.Sprintf("Deployment %s 重启指令已成功下发", req.Deployment)})
}

// ---------- 上传接口 ----------

func (s *Server) createUpload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FileName string `json:"fileName"`
		Size     int64  `json:"size"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	u, err := s.engine.Uploads.Create(req.FileName, req.Size, "local-user")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, u.Snapshot())
}

func (s *Server) getUpload(w http.ResponseWriter, r *http.Request) {
	if err := checkID(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	u, err := s.engine.Uploads.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, u.Snapshot())
}

func (s *Server) writeChunk(w http.ResponseWriter, r *http.Request) {
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		writeErr(w, http.StatusBadRequest, "offset 参数错误")
		return
	}
	if err := checkID(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxChunkSize)
	received, err := s.engine.Uploads.WriteChunk(r.PathValue("id"), offset, body)
	if err != nil {
		code := http.StatusConflict
		if errors.Is(err, upload.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeJSON(w, code, map[string]any{"error": err.Error(), "received": received})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"received": received})
}

func (s *Server) completeUpload(w http.ResponseWriter, r *http.Request) {
	if err := checkID(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	u, err := s.engine.Uploads.Complete(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, u.Snapshot())
}

// ---------- 发布流水线 ----------

func (s *Server) listReleases(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	appFilter := r.URL.Query().Get("app")
	if appFilter != "" && !config.IDRe.MatchString(appFilter) {
		writeErr(w, http.StatusBadRequest, "app 参数不合法")
		return
	}
	writeJSON(w, http.StatusOK, s.engine.Store.List(appFilter, limit))
}

func (s *Server) startRelease(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AppID             string `json:"appId"`
		Tag               string `json:"tag"`
		UploadID          string `json:"uploadId"`
		RunLocalBuild     bool   `json:"runLocalBuild"`
		UseLocalArtifact  bool   `json:"useLocalArtifact"`
		LocalArtifactPath string `json:"localArtifactPath"`
		BuildDir          string `json:"buildDir"`
		BuildCmd          string `json:"buildCmd"`
		Force             bool   `json:"force"`
		NoReuse           bool   `json:"noReuse"`
		Remark            string `json:"remark"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// 如果用户在发布界面直接选定了新的本地产物路径，自动持久化保存到该应用配置中，下次打开自动复用
	if p := strings.TrimSpace(req.LocalArtifactPath); p != "" {
		// 产物路径决定将被上传到远程的文件，拒绝包含控制字符的异常输入
		if strings.ContainsFunc(p, unicode.IsControl) {
			writeErr(w, http.StatusBadRequest, "本地产物路径包含非法字符")
			return
		}
		// 只接受绝对路径：来源必须是文件选择器或用户显式填写的完整路径
		if exp := config.ExpandHome(p); !filepath.IsAbs(exp) {
			writeErr(w, http.StatusBadRequest, "本地产物路径必须是绝对路径")
			return
		}
		if err := s.cfg.UpdateLocalArtifactPath(req.AppID, p); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	rel, err := s.engine.StartDeployContext(r.Context(), pipeline.StartRequest{
		AppID:            req.AppID,
		Tag:              strings.TrimSpace(req.Tag),
		UploadID:         req.UploadID,
		RunLocalBuild:    req.RunLocalBuild,
		UseLocalArtifact: req.UseLocalArtifact,
		Force:            req.Force,
		NoReuse:          req.NoReuse,
		Remark:           req.Remark,
		BuildDir:         req.BuildDir,
		BuildCmd:         req.BuildCmd,
	})
	if err != nil {
		var ce *pipeline.ConflictError
		if errors.As(err, &ce) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":    ce.Error(),
				"precheck": ce.Result,
			})
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, rel)
}

func (s *Server) rollbackRelease(w http.ResponseWriter, r *http.Request) {
	id, ok := releaseID(w, r)
	if !ok {
		return
	}
	var req struct {
		Remark string `json:"remark"`
	}
	_ = readJSON(r, &req)

	rel, err := s.engine.StartRollback(pipeline.RollbackRequest{
		ReleaseID: id,
		Remark:    req.Remark,
	})
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, rel)
}

func (s *Server) getRelease(w http.ResponseWriter, r *http.Request) {
	id, ok := releaseID(w, r)
	if !ok {
		return
	}
	rel, err := s.engine.Store.Get(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rel)
}

func (s *Server) cancelRelease(w http.ResponseWriter, r *http.Request) {
	id, ok := releaseID(w, r)
	if !ok {
		return
	}
	if err := s.engine.Cancel(id); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) releaseLog(w http.ResponseWriter, r *http.Request) {
	id, ok := releaseID(w, r)
	if !ok {
		return
	}
	rel, err := s.engine.Store.Get(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// 下载文件名带上服务名、版本号与发布时间：浏览器以 Content-Disposition 为准，
	// 前端 <a download> 的同名设置会被此头覆盖，所以必须由服务端给出有意义的文件名
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", releaseLogFileName(rel)))
	http.ServeFile(w, r, s.engine.LogPath(id))
}

// releaseLogFileName 生成发布日志下载文件名：服务名_版本号_发布时间.log
func releaseLogFileName(rel *store.Release) string {
	safe := func(s string) string {
		var b strings.Builder
		for _, r := range s {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
				b.WriteRune(r)
			} else {
				b.WriteByte('_')
			}
		}
		if b.Len() == 0 {
			return "release"
		}
		return b.String()
	}
	ts := rel.CreatedAt.Format("20060102-150405")
	if rel.CreatedAt.IsZero() {
		ts = rel.ID
	}
	return fmt.Sprintf("%s_%s_%s.log", safe(rel.App), safe(rel.Tag), ts)
}

func (s *Server) releaseEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := releaseID(w, r)
	if !ok {
		return
	}
	rel, err := s.engine.Store.Get(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "不支持流式响应")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	rc := http.NewResponseController(w)
	// 每次写入前设置写截止时间：客户端半开/卡死时不会让 goroutine 与订阅无限挂起
	send := func(event string, data any) error {
		b, _ := json.Marshal(data)
		_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
		_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
		return err
	}
	write := func(text string) error {
		_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
		_, err := io.WriteString(w, text)
		if err == nil {
			flusher.Flush()
		}
		return err
	}

	var history []string
	var ch chan pipeline.Event
	if hub := s.engine.Hub(id); hub != nil {
		history, ch = hub.Subscribe()
		if ch != nil {
			defer hub.Unsubscribe(ch)
		}
		// 订阅与 Hub 关闭之间存在空窗：ch 为 nil 时补取最新终态，避免前端停留在 running
		if latest, err := s.engine.Store.Get(id); err == nil {
			rel = latest
		}
	} else {
		history = readLines(s.engine.LogPath(id))
	}
	if err := send("release", rel); err != nil {
		return
	}
	for _, l := range history {
		if err := send("log", l); err != nil {
			return
		}
	}
	if err := write(""); err != nil {
		return
	}
	if ch == nil {
		_ = send("end", struct{}{})
		_ = write("")
		return
	}

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			if err := write(": ping\n\n"); err != nil {
				return
			}
		case ev, ok := <-ch:
			if !ok {
				if latest, err := s.engine.Store.Get(id); err == nil {
					_ = send("release", latest)
				}
				_ = send("end", struct{}{})
				_ = write("")
				return
			}
			if err := send(ev.Type, ev.Data); err != nil {
				return
			}
			if err := write(""); err != nil {
				return
			}
			if ev.Type == "end" {
				return
			}
		}
	}
}

// maxReadLines 回放已完成发布日志时只读最后 N 行（与 Hub 历史上限一致）
const maxReadLines = 5000

func readLines(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	// 环形缓冲：下标取模写入，最后按顺序拼接，避免逐行搬运整个窗口
	ring := make([]string, maxReadLines)
	total := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		ring[total%maxReadLines] = sc.Text()
		total++
	}
	if total <= maxReadLines {
		return append([]string(nil), ring[:total]...)
	}
	out := make([]string, 0, maxReadLines)
	start := total % maxReadLines
	out = append(out, ring[start:]...)
	return append(out, ring[:start]...)
}

// sanitizePrompt 用户输入会被拼进 AppleScript 字符串或作为 zenity 参数，剔除引号、反斜杠与控制字符
func sanitizePrompt(p string) string {
	p = strings.Map(func(r rune) rune {
		if r == '"' || r == '\\' || unicode.IsControl(r) {
			return -1
		}
		return r
	}, p)
	if r := []rune(p); len(r) > 60 {
		p = string(r[:60])
	}
	return p
}
