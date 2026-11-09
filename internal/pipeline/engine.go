package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/template"
	"time"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/k8s"
	"build-to-deploy/internal/sshpool"
	"build-to-deploy/internal/store"
	"build-to-deploy/internal/templates"
	"build-to-deploy/internal/upload"
)

type Engine struct {
	Cfg     *config.Config
	Store   *store.Store
	Uploads *upload.Manager
	SSH     *sshpool.Pool
	Kube    *k8s.Client
	DataDir string

	mu      sync.Mutex
	running map[string]*activeRun // appID -> activeRun
	hubs    map[string]*Hub       // releaseID -> Hub
}

type activeRun struct {
	releaseID string
	cancel    context.CancelFunc
}

func NewEngine(cfg *config.Config, st *store.Store, ups *upload.Manager, pool *sshpool.Pool, kube *k8s.Client, dataDir string) *Engine {
	e := &Engine{
		Cfg:     cfg,
		Store:   st,
		Uploads: ups,
		SSH:     pool,
		Kube:    kube,
		DataDir: dataDir,
		running: make(map[string]*activeRun),
		hubs:    make(map[string]*Hub),
	}
	_ = os.MkdirAll(e.logDir(), 0755)
	return e
}

func (e *Engine) logDir() string   { return filepath.Join(e.DataDir, "logs") }
func (e *Engine) LogPath(id string) string { return filepath.Join(e.logDir(), id+".log") }

type Conflict struct {
	Source  string `json:"source"` // docker | k8s
	Message string `json:"message"`
}

type PrecheckResult struct {
	Conflicts  []Conflict `json:"conflicts"`
	Warnings   []string   `json:"warnings"`
	Current    string     `json:"current"`
	CurrentTag string     `json:"currentTag"`
}

type ConflictError struct {
	Result *PrecheckResult
}

func (c *ConflictError) Error() string {
	return "版本已存在，需弹窗确认覆盖"
}

// Precheck 检查目标服务器上该 Tag 是否存在
func (e *Engine) Precheck(ctx context.Context, app *config.AppConfig, tag string) (*PrecheckResult, error) {
	if !config.TagRe.MatchString(tag) {
		return nil, errors.New("Tag 格式不合法：只能包含字母、数字、. _ -，且不超过 128 个字符")
	}

	res := &PrecheckResult{
		Conflicts: []Conflict{},
		Warnings:  []string{},
	}

	srv := e.Cfg.FindServer(app.ServerID)
	if srv == nil {
		return nil, fmt.Errorf("找不到对应的 SSH 服务器配置 [%s]", app.ServerID)
	}

	if e.Cfg.Server.DryRun {
		res.Warnings = append(res.Warnings, "演练模式 (DryRun)：不连接真实远程主机")
		return res, nil
	}

	// 1. 检查服务器本地是否已有此镜像 tag
	ref := fmt.Sprintf("%s:%s", app.Image, tag)
	imgCheckCmd := fmt.Sprintf("docker images -q %s", ref)
	imgId, err := e.SSH.ExecuteOutput(ctx, srv, imgCheckCmd)
	if err == nil && strings.TrimSpace(imgId) != "" {
		res.Conflicts = append(res.Conflicts, Conflict{
			Source:  "docker",
			Message: fmt.Sprintf("服务器本地已存在镜像 %s (ID: %s)", ref, strings.TrimSpace(imgId)[:12]),
		})
	}

	// 2. 检查 K8s 当前 Deployment 的镜像版本
	k8sCheckCmd := fmt.Sprintf("kubectl -n %s get deployment %s -o jsonpath='{.spec.template.spec.containers[?(@.name==\"%s\")].image}'", app.Namespace, app.Deployment, app.Container)
	curImg, err := e.SSH.ExecuteOutput(ctx, srv, k8sCheckCmd)
	if err == nil && strings.TrimSpace(curImg) != "" {
		res.Current = strings.TrimSpace(curImg)
		res.CurrentTag = extractTag(res.Current)
		if res.CurrentTag == tag {
			res.Conflicts = append(res.Conflicts, Conflict{
				Source:  "k8s",
				Message: fmt.Sprintf("K8s 线上当前正在运行此版本 (%s)", ref),
			})
		}
	} else if err != nil {
		res.Warnings = append(res.Warnings, "获取 K8s 当前镜像状态跳过: "+err.Error())
	}

	return res, nil
}

func extractTag(image string) string {
	if i := strings.LastIndex(image, ":"); i > strings.LastIndex(image, "/") {
		return image[i+1:]
	}
	return "latest"
}

type StartRequest struct {
	AppID    string
	Tag      string
	UploadID string
	Force    bool
	Remark   string
}

func (e *Engine) StartDeploy(req StartRequest) (*store.Release, error) {
	app := e.Cfg.FindApp(req.AppID)
	if app == nil {
		return nil, fmt.Errorf("应用 %s 不存在", req.AppID)
	}

	srv := e.Cfg.FindServer(app.ServerID)
	if srv == nil {
		return nil, fmt.Errorf("未找到服务器配置: %s", app.ServerID)
	}

	e.mu.Lock()
	if r, ok := e.running[app.ID]; ok {
		e.mu.Unlock()
		return nil, fmt.Errorf("应用 %s 正在发布中 (%s)，请稍候", app.Name, r.releaseID)
	}
	run := &activeRun{}
	e.running[app.ID] = run
	e.mu.Unlock()

	ok := false
	defer func() {
		if !ok {
			e.mu.Lock()
			delete(e.running, app.ID)
			e.mu.Unlock()
		}
	}()

	pctx, pcancel := context.WithTimeout(context.Background(), 20*time.Second)
	pre, err := e.Precheck(pctx, app, req.Tag)
	pcancel()
	if err != nil {
		return nil, err
	}

	if len(pre.Conflicts) > 0 && !req.Force {
		return nil, &ConflictError{Result: pre}
	}

	localArtifact := filepath.Join(e.DataDir, "uploads", req.UploadID+".part")
	up, err := e.Uploads.Get(req.UploadID)
	if err != nil {
		return nil, err
	}

	relID := upload.NewID()
	stages := []store.Stage{
		{Name: "validate", Title: "校验本地产物", Status: store.StatusPending},
		{Name: "upload", Title: "SFTP 上传至产物目录", Status: store.StatusPending},
		{Name: "sync", Title: "准备构建上下文", Status: store.StatusPending},
		{Name: "build", Title: "执行镜像构建", Status: store.StatusPending},
	}
	if strings.TrimSpace(app.PushCmd) != "" {
		stages = append(stages, store.Stage{Name: "push", Title: "推送镜像", Status: store.StatusPending})
	}
	stages = append(stages,
		store.Stage{Name: "deploy", Title: "执行 K8s 部署", Status: store.StatusPending},
		store.Stage{Name: "rollout", Title: "等待 Pod 就绪", Status: store.StatusPending},
	)

	rel := &store.Release{
		ID:         relID,
		App:        app.ID,
		Kind:       "deploy",
		Tag:        req.Tag,
		Image:      fmt.Sprintf("%s:%s", app.Image, req.Tag),
		FileName:   up.FileName,
		FileSize:   up.Size,
		FileSHA256: up.SHA256,
		Force:      req.Force && len(pre.Conflicts) > 0,
		Operator:   srv.User + "@" + srv.Host,
		Remark:     req.Remark,
		PrevImage:  pre.Current,
		Status:     store.StatusRunning,
		CreatedAt:  time.Now(),
		Stages:     stages,
	}

	if err := e.Store.Add(rel); err != nil {
		return nil, err
	}
	ok = true

	ctx, cancel := context.WithCancel(context.Background())
	hub := newHub(e.LogPath(rel.ID))

	e.mu.Lock()
	run.releaseID = rel.ID
	run.cancel = cancel
	e.hubs[rel.ID] = hub
	e.mu.Unlock()

	go func() {
		defer cancel()
		defer func() {
			e.mu.Lock()
			delete(e.running, app.ID)
			e.mu.Unlock()
		}()

		runner := &pipelineRunner{
			engine:   e,
			ctx:      ctx,
			rel:      rel,
			app:      app,
			srv:      srv,
			hub:      hub,
			artifact: localArtifact,
			precheck: pre,
		}
		err := runner.run()
		runner.finish(err)
		hub.Close()
	}()

	return rel, nil
}

type pipelineRunner struct {
	engine   *Engine
	ctx      context.Context
	rel      *store.Release
	app      *config.AppConfig
	srv      *sshpool.ServerConfig
	hub      *Hub
	artifact string
	precheck *PrecheckResult
}

func (r *pipelineRunner) update(fn func(rel *store.Release)) {
	rel, err := r.engine.Store.Update(r.rel.ID, fn)
	if err == nil {
		r.hub.Publish(Event{Type: "release", Data: rel})
	}
}

func (r *pipelineRunner) stage(name string, fn func(w io.Writer) error) error {
	now := time.Now()
	var title string
	r.update(func(rel *store.Release) {
		for i := range rel.Stages {
			if rel.Stages[i].Name == name {
				rel.Stages[i].Status = store.StatusRunning
				rel.Stages[i].StartedAt = &now
				title = rel.Stages[i].Title
			}
		}
	})
	r.hub.Logf(fmt.Sprintf("\n==> 阶段 [%s]: %s", name, title))
	err := fn(r.hub)
	if err == nil && r.ctx.Err() != nil {
		err = r.ctx.Err()
	}

	status := store.StatusSuccess
	if err != nil {
		if errors.Is(err, context.Canceled) {
			status = store.StatusCanceled
		} else {
			status = store.StatusFailed
		}
	}
	end := time.Now()
	r.update(func(rel *store.Release) {
		for i := range rel.Stages {
			if rel.Stages[i].Name == name {
				rel.Stages[i].Status = status
				rel.Stages[i].FinishedAt = &end
			}
		}
	})
	if err != nil {
		r.hub.Logf(fmt.Sprintf("[错误] 阶段 %s 执行失败: %v", title, err))
	}
	return err
}

func (r *pipelineRunner) renderCmd(tplStr string) (string, error) {
	t, err := template.New("cmd").Parse(tplStr)
	if err != nil {
		return "", err
	}
	yamlPath := r.app.K8sYamlDir
	if r.app.K8sYamlFile != "" {
		if r.app.K8sYamlDir != "" {
			yamlPath = strings.TrimRight(r.app.K8sYamlDir, "/") + "/" + r.app.K8sYamlFile
		} else {
			yamlPath = r.app.K8sYamlFile
		}
	}
	data := map[string]any{
		"ArtifactDir": r.app.ArtifactDir,
		"BuildDir":    r.app.BuildDir,
		"K8sYamlDir":  r.app.K8sYamlDir,
		"K8sYamlFile": r.app.K8sYamlFile,
		"K8sYamlPath": yamlPath,
		"Image":       r.app.Image,
		"Tag":         r.rel.Tag,
		"Namespace":   r.app.Namespace,
		"Deployment":  r.app.Deployment,
		"Container":   r.app.Container,
		"FileName":    r.rel.FileName,
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (r *pipelineRunner) run() error {
	r.hub.Logf(fmt.Sprintf("==> 开始发布应用: %s [版本: %s]", r.app.Name, r.rel.Tag))
	r.hub.Logf(fmt.Sprintf("==> 目标服务器: %s@%s:%d", r.srv.User, r.srv.Host, r.srv.Port))
	r.hub.Logf(fmt.Sprintf("==> 产物目录: %s", r.app.ArtifactDir))
	r.hub.Logf(fmt.Sprintf("==> 构建目录: %s", r.app.BuildDir))
	if r.app.K8sYamlFile != "" {
		r.hub.Logf(fmt.Sprintf("==> K8s配置:  %s/%s", r.app.K8sYamlDir, r.app.K8sYamlFile))
	} else {
		r.hub.Logf(fmt.Sprintf("==> K8s目录:  %s", r.app.K8sYamlDir))
	}

	// 1. 校验产物
	if err := r.stage("validate", func(w io.Writer) error {
		return validateArtifact(w, r.app.Type, r.artifact)
	}); err != nil {
		return err
	}

	// 2. 上传到服务器产物目录
	remoteArtifactPath := fmt.Sprintf("%s/%s", r.app.ArtifactDir, r.rel.FileName)
	if err := r.stage("upload", func(w io.Writer) error {
		if r.engine.Cfg.Server.DryRun {
			fmt.Fprintf(w, "[dry-run] 模拟通过 SFTP 上传 %s -> %s\n", r.rel.FileName, remoteArtifactPath)
			time.Sleep(500 * time.Millisecond)
			return nil
		}
		fmt.Fprintf(w, "正在通过 SFTP 上传到远程服务器: %s ...\n", remoteArtifactPath)
		return r.engine.SSH.UploadFile(r.ctx, r.srv, r.artifact, remoteArtifactPath, func(up, total int64) {
			pct := float64(up) / float64(total) * 100
			if up%int64(1024*1024*2) == 0 || up == total {
				fmt.Fprintf(w, "上传进度: %.1f%% (%d/%d bytes)\n", pct, up, total)
			}
		})
	}); err != nil {
		return err
	}

	// 3. 准备构建上下文（将产物从 ArtifactDir 拷贝到 BuildDir）
	if err := r.stage("sync", func(w io.Writer) error {
		targetName := templates.ArtifactName(r.app.Type)
		syncCmd := fmt.Sprintf("mkdir -p %s && cp -f %s %s/%s", r.app.BuildDir, remoteArtifactPath, r.app.BuildDir, targetName)
		if r.engine.Cfg.Server.DryRun {
			fmt.Fprintf(w, "[dry-run] $ %s\n", syncCmd)
			return nil
		}
		return r.engine.SSH.RunCommand(r.ctx, r.srv, syncCmd, w)
	}); err != nil {
		return err
	}

	// 4. 执行镜像构建
	if err := r.stage("build", func(w io.Writer) error {
		cmd, err := r.renderCmd(r.app.BuildCmd)
		if err != nil {
			return fmt.Errorf("渲染构建命令失败: %w", err)
		}
		if r.engine.Cfg.Server.DryRun {
			fmt.Fprintf(w, "[dry-run] $ %s\n", cmd)
			time.Sleep(1 * time.Second)
			fmt.Fprintln(w, "[dry-run] 镜像构建成功: "+r.rel.Image)
			return nil
		}
		return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
	}); err != nil {
		return err
	}

	// 5. 推送镜像（如果有配置 pushCmd）
	if strings.TrimSpace(r.app.PushCmd) != "" {
		if err := r.stage("push", func(w io.Writer) error {
			cmd, err := r.renderCmd(r.app.PushCmd)
			if err != nil {
				return fmt.Errorf("渲染推送命令失败: %w", err)
			}
			if r.engine.Cfg.Server.DryRun {
				fmt.Fprintf(w, "[dry-run] $ %s\n", cmd)
				return nil
			}
			return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
		}); err != nil {
			return err
		}
	}

	// 6. 执行 K8s 部署
	if err := r.stage("deploy", func(w io.Writer) error {
		cmd, err := r.renderCmd(r.app.DeployCmd)
		if err != nil {
			return fmt.Errorf("渲染部署命令失败: %w", err)
		}
		if r.engine.Cfg.Server.DryRun {
			fmt.Fprintf(w, "[dry-run] $ %s\n", cmd)
			return nil
		}
		return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
	}); err != nil {
		return err
	}

	// 7. 等待 Pod 滚动就绪
	if err := r.stage("rollout", func(w io.Writer) error {
		return r.engine.Kube.RolloutStatus(r.ctx, w, r.srv, r.app.Namespace, r.app.Deployment, r.app.RolloutTimeout)
	}); err != nil {
		// 自动回滚逻辑
		if r.app.AutoRollback && r.rel.PrevImage != "" && r.rel.PrevImage != r.rel.Image {
			r.hub.Logf(fmt.Sprintf("\n[警告] 服务部署未就绪，触发自动回滚到上一版本: %s", r.rel.PrevImage))
			rbCmd := fmt.Sprintf("kubectl -n %s set image deployment/%s %s=%s", r.app.Namespace, r.app.Deployment, r.app.Container, r.rel.PrevImage)
			_ = r.engine.SSH.RunCommand(context.Background(), r.srv, rbCmd, r.hub)
		}
		return err
	}

	return nil
}

func (r *pipelineRunner) finish(err error) {
	now := time.Now()
	status := store.StatusSuccess
	if err != nil {
		if errors.Is(err, context.Canceled) {
			status = store.StatusCanceled
		} else {
			status = store.StatusFailed
		}
	}
	r.update(func(rel *store.Release) {
		rel.Status = status
		rel.FinishedAt = &now
		if err != nil {
			rel.Error = err.Error()
		}
	})
	if status == store.StatusSuccess {
		r.hub.Logf(fmt.Sprintf("\n==> 🎉 发布成功! 镜像已上线: %s", r.rel.Image))
	} else {
		r.hub.Logf(fmt.Sprintf("\n==> ❌ 发布终止: %v", err))
	}
}

func (e *Engine) Hub(id string) *Hub {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.hubs[id]
}

func (e *Engine) Cancel(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range e.running {
		if r.releaseID == id {
			r.cancel()
			return nil
		}
	}
	return errors.New("该发布未在运行")
}

func validateArtifact(w io.Writer, appType, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("无法读取产物文件: %w", err)
	}
	defer f.Close()

	head := make([]byte, 4)
	if _, err := io.ReadFull(f, head); err != nil {
		return errors.New("产物文件为空或太小")
	}

	switch appType {
	case "jar":
		if string(head) != "PK\x03\x04" {
			return errors.New("不是合法的 jar 文件（ZIP 魔数不匹配）")
		}
		fmt.Fprintln(w, "✔ jar 文件校验通过")
	case "go", "binary":
		if string(head) != "\x7fELF" {
			return errors.New("不是合法的 Linux ELF 二进制可执行文件 (请用 GOOS=linux GOARCH=amd64 编译)")
		}
		fmt.Fprintln(w, "✔ Linux ELF 二进制校验通过")
	default:
		fmt.Fprintln(w, "✔ 产物文件读取成功")
	}
	return nil
}
