package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
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

// dryRun 每次读取当前配置：网页上切换演练模式后无需重启即可生效
func (e *Engine) dryRun() bool { return e.Cfg.ServerSettingsSnapshot().DryRun }

func (e *Engine) logDir() string           { return filepath.Join(e.DataDir, "logs") }
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

	if e.dryRun() {
		e.SSH.MarkSimulated(srv)
		res.Warnings = append(res.Warnings, "演练模式 (DryRun)：不连接真实远程主机")
		return res, nil
	}

	// 1. 检查服务器本地是否已有此镜像 tag
	ref := fmt.Sprintf("%s:%s", app.Image, tag)
	imgCheckCmd := fmt.Sprintf("docker images -q %s", shellQuote(ref))
	imgId, err := e.SSH.ExecuteOutput(ctx, srv, imgCheckCmd)
	if err == nil && strings.TrimSpace(imgId) != "" {
		id := firstN(strings.TrimSpace(imgId), 12)
		res.Conflicts = append(res.Conflicts, Conflict{
			Source:  "docker",
			Message: fmt.Sprintf("服务器本地已存在镜像 %s (ID: %s)", ref, id),
		})
	}

	// 2. 检查 K8s 当前 Deployment 的镜像版本（变量全部经过白名单校验并 shell 转义）
	k8sCheckCmd := fmt.Sprintf("kubectl -n %s get deployment %s -o jsonpath=%s",
		shellQuote(app.Namespace), shellQuote(app.Deployment),
		shellQuote(fmt.Sprintf(`{.spec.template.spec.containers[?(@.name=="%s")].image}`, app.Container)))
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

// firstN 取字符串前 n 个字节；输出可能为空或非 ASCII，直接切片会越界
func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// imageRefRe 镜像引用白名单（含 tag / digest）。Precheck 结果会进入回滚命令，必须校验
var imageRefRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/:@-]*$`)

// shellQuote 将变量安全地拼入远程 sh 命令：单引号包裹并转义内部单引号
func shellQuote(s string) string { return sshpool.ShellQuote(s) }

// isYamlPath 统一判断远程路径是否为 K8s YAML 文件（大小写不敏感，两处调用保持一致）
func isYamlPath(p string) bool {
	lower := strings.ToLower(p)
	return strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml")
}

type StartRequest struct {
	AppID            string
	Tag              string
	UploadID         string
	RunLocalBuild    bool
	UseLocalArtifact bool
	Force            bool
	Remark           string
}

func fileStatAndSHA256(path string) (string, int64, string, error) {
	st, err := os.Stat(path)
	if err != nil {
		return "", 0, "", err
	}
	if st.IsDir() {
		return "", 0, "", fmt.Errorf("产物路径 %s 是一个目录，请指定具体文件", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", 0, "", err
	}
	return filepath.Base(path), st.Size(), hex.EncodeToString(h.Sum(nil)), nil
}

func (e *Engine) StartDeploy(req StartRequest) (*store.Release, error) {
	app := e.Cfg.FindApp(req.AppID)
	if app == nil {
		return nil, fmt.Errorf("应用 %s 不存在", req.AppID)
	}
	// 模板变量会被直接渲染进远程 shell 命令：保存时校验过，这里兜底手改 YAML / 旧配置的绕过
	if err := validateAppForPipeline(app); err != nil {
		return nil, err
	}

	srv := e.Cfg.FindServer(app.ServerID)
	if srv == nil {
		return nil, fmt.Errorf("未找到服务器配置: %s", app.ServerID)
	}

	// 在注册 running 之前生成 release ID 与 ctx，保证同一临界区内 Cancel 即可命中（避免预检期间无法取消）
	relID := upload.NewID()
	ctx, cancel := context.WithCancel(context.Background())
	run := &activeRun{releaseID: relID, cancel: cancel}

	e.mu.Lock()
	if r, ok := e.running[app.ID]; ok {
		e.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("应用 %s 正在发布中 (%s)，请稍候", app.Name, r.releaseID)
	}
	e.running[app.ID] = run
	e.mu.Unlock()

	ok := false
	defer func() {
		if !ok {
			cancel()
			e.mu.Lock()
			delete(e.running, app.ID)
			e.mu.Unlock()
		}
	}()

	// 预检的超时比远端操作宽松，且可被用户取消
	pctx, pcancel := context.WithTimeout(ctx, 60*time.Second)
	pre, err := e.Precheck(pctx, app, req.Tag)
	pcancel()
	if err != nil {
		return nil, err
	}

	if len(pre.Conflicts) > 0 && !req.Force {
		return nil, &ConflictError{Result: pre}
	}

	var (
		localArtifact string
		fileName      string
		fileSize      int64
		fileSHA256    string
		stages        []store.Stage
	)

	if req.RunLocalBuild {
		if strings.TrimSpace(app.LocalBuildCmd) == "" {
			return nil, errors.New("该应用尚未配置本地编译命令 (LocalBuildCmd)，请先在应用配置中填写")
		}
		localArtifact = app.ResolveLocalArtifactPath()
		if localArtifact == "" {
			return nil, errors.New("该应用尚未配置本地编译产物路径 (LocalArtifactPath)，请先在应用配置中填写")
		}
		fileName = filepath.Base(localArtifact)
		stages = append(stages, store.Stage{Name: "local_build", Title: "执行本地编译", Status: store.StatusPending})
	} else if req.UploadID != "" {
		localArtifact = filepath.Join(e.DataDir, "uploads", req.UploadID+".part")
		up, err := e.Uploads.Get(req.UploadID)
		if err != nil {
			return nil, err
		}
		// 分片未上传完成时禁止发布：否则远端落的是残缺包，且 SHA256 为空
		if !up.Done {
			return nil, fmt.Errorf("上传任务 %s 尚未完成（已接收 %d/%d），请等待分片上传完成后再发布", req.UploadID, up.Received, up.Size)
		}
		fileName = up.FileName
		fileSize = up.Size
		fileSHA256 = up.SHA256
	} else if req.UseLocalArtifact {
		localArtifact = app.ResolveLocalArtifactPath()
		if localArtifact == "" {
			return nil, errors.New("未选择上传文件，且应用未配置默认本地产物路径 (LocalArtifactPath)")
		}
		name, sz, sum, err := fileStatAndSHA256(localArtifact)
		if err != nil {
			if e.dryRun() {
				fileName = filepath.Base(localArtifact)
			} else {
				return nil, fmt.Errorf("读取本地默认产物失败 (%s): %w", localArtifact, err)
			}
		} else {
			fileName = name
			fileSize = sz
			fileSHA256 = sum
		}
	} else {
		return nil, errors.New("请选择本地产物文件或开启本地一键编译")
	}

	// 文件名会被拼入远程 cp 命令，统一校验（上传接口已校验，这里兜底本地产物场景）
	if err := upload.ValidateFileName(fileName); err != nil {
		return nil, err
	}

	stages = append(stages,
		store.Stage{Name: "validate", Title: "校验本地产物", Status: store.StatusPending},
		store.Stage{Name: "upload", Title: "SFTP 上传至产物目录", Status: store.StatusPending},
		store.Stage{Name: "sync", Title: "准备构建上下文", Status: store.StatusPending},
		store.Stage{Name: "build", Title: "执行镜像构建", Status: store.StatusPending},
	)
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
		FileName:   fileName,
		FileSize:   fileSize,
		FileSHA256: fileSHA256,
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

	hub := newHub(e.LogPath(rel.ID))

	e.mu.Lock()
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
			engine:        e,
			ctx:           ctx,
			rel:           rel,
			app:           app,
			srv:           srv,
			hub:           hub,
			artifact:      localArtifact,
			runLocalBuild: req.RunLocalBuild,
			precheck:      pre,
		}
		err := runner.run()
		runner.finish(err)
		hub.Close()
		e.dropHub(rel.ID)
	}()

	return rel, nil
}

type RollbackRequest struct {
	ReleaseID string `json:"releaseId"`
	Remark    string `json:"remark"`
}

// StartRollback 触发历史版本一键回滚
func (e *Engine) StartRollback(req RollbackRequest) (*store.Release, error) {
	target, err := e.Store.Get(req.ReleaseID)
	if err != nil {
		return nil, fmt.Errorf("未找到对应的发布记录: %w", err)
	}
	app := e.Cfg.FindApp(target.App)
	if app == nil {
		return nil, fmt.Errorf("应用 %s 不存在", target.App)
	}
	srv := e.Cfg.FindServer(app.ServerID)
	if srv == nil {
		return nil, fmt.Errorf("未找到服务器配置: %s", app.ServerID)
	}

	relID := upload.NewID()
	ctx, cancel := context.WithCancel(context.Background())
	run := &activeRun{releaseID: relID, cancel: cancel}

	e.mu.Lock()
	if r, ok := e.running[app.ID]; ok {
		e.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("应用 %s 正在发布中 (%s)，请稍候", app.Name, r.releaseID)
	}
	e.running[app.ID] = run
	e.mu.Unlock()

	ok := false
	defer func() {
		if !ok {
			cancel()
			e.mu.Lock()
			delete(e.running, app.ID)
			e.mu.Unlock()
		}
	}()

	// 发布记录会被回滚流水线再次使用，其中的镜像引用会进入远程命令，必须与表单输入同样校验
	if err := validateAppForPipeline(app); err != nil {
		return nil, err
	}
	// 历史记录的镜像引用会被直接写回远程 YAML，非法值会损坏配置文件
	if !imageRefRe.MatchString(target.Image) {
		return nil, fmt.Errorf("历史记录镜像引用不合法 (%q)，无法回滚", target.Image)
	}

	stages := []store.Stage{
		{Name: "deploy", Title: "执行 K8s 镜像更新", Status: store.StatusPending},
		{Name: "rollout", Title: "等待 Pod 滚动就绪", Status: store.StatusPending},
	}

	remark := req.Remark
	if strings.TrimSpace(remark) == "" {
		remark = fmt.Sprintf("一键回滚至历史版本 [%s]", target.Tag)
	}

	rel := &store.Release{
		ID:         relID,
		App:        app.ID,
		Kind:       "rollback",
		Tag:        target.Tag,
		Image:      target.Image,
		FileName:   target.FileName,
		FileSize:   target.FileSize,
		FileSHA256: target.FileSHA256,
		Force:      true,
		RollbackOf: target.ID,
		Operator:   srv.User + "@" + srv.Host,
		Remark:     remark,
		Status:     store.StatusRunning,
		CreatedAt:  time.Now(),
		Stages:     stages,
	}

	if err := e.Store.Add(rel); err != nil {
		return nil, err
	}
	ok = true

	hub := newHub(e.LogPath(rel.ID))

	e.mu.Lock()
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
			engine: e,
			ctx:    ctx,
			rel:    rel,
			app:    app,
			srv:    srv,
			hub:    hub,
		}
		err := runner.runRollback()
		runner.finish(err)
		hub.Close()
		e.dropHub(rel.ID)
	}()

	return rel, nil
}

type pipelineRunner struct {
	engine        *Engine
	ctx           context.Context
	rel           *store.Release
	app           *config.AppConfig
	srv           *sshpool.ServerConfig
	hub           *Hub
	artifact      string
	runLocalBuild bool
	precheck      *PrecheckResult
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

func (r *pipelineRunner) k8sYamlFilePath() string {
	yamlPath := strings.TrimSpace(r.app.K8sYamlDir)
	if r.app.K8sYamlFile != "" {
		if yamlPath != "" {
			yamlPath = strings.TrimRight(yamlPath, "/") + "/" + strings.TrimLeft(r.app.K8sYamlFile, "/")
		} else {
			yamlPath = r.app.K8sYamlFile
		}
	}
	return yamlPath
}

// splitImagePrefix 把 "image:   值  # 注释" 拆成前缀（含冒号后原有空白）、值与行尾注释
func splitImagePrefix(line string) (prefix, value, suffix string, ok bool) {
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "#") {
		return "", "", "", false
	}
	if !strings.HasPrefix(trimmed, "image:") && !strings.HasPrefix(trimmed, "- image:") {
		return "", "", "", false
	}
	idx := strings.Index(line, "image:")
	after := idx + len("image:")
	rest := line[after:]
	// 只剥掉左侧空白，写回时用原始空白拼接，保证缩进不变
	lead := len(rest) - len(strings.TrimLeft(rest, " \t"))
	rest = rest[lead:]
	// 保留行尾注释（值后至少两个空格开始的 #）
	if c := strings.Index(rest, " #"); c >= 0 {
		suffix = rest[c:]
		rest = rest[:c]
	}
	value = strings.Trim(strings.TrimSpace(rest), `"'`)
	return line[:after] + strings.Repeat(" ", lead), value, suffix, true
}

// imageRepoOf 去掉 tag / digest 得到镜像仓库名
func imageRepoOf(ref string) string {
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		ref = ref[:i]
	}
	return ref
}

// replaceYamlImage 在 K8s YAML 内容中精准替换目标容器的 image 字段值，保留原有缩进与注释。
// 只做完整镜像引用匹配，避免把 "harbor/app/gateway-sts" 这类同前缀的 sidecar 镜像误改。
func replaceYamlImage(content, imageRepo, targetImage string) (string, bool) {
	lines := strings.Split(content, "\n")
	replaced := false

	// 第一轮：精确匹配同一镜像仓库的引用
	if imageRepo != "" {
		for i, line := range lines {
			prefix, value, suffix, ok := splitImagePrefix(line)
			if !ok || imageRepoOf(value) != imageRepo {
				continue
			}
			lines[i] = prefix + " " + targetImage + suffix
			replaced = true
		}
	}

	// 第二轮：如果 YAML 中原本写的是其他旧镜像名，则替换第一个非注释的 image: 行
	if !replaced {
		for i, line := range lines {
			prefix, _, suffix, ok := splitImagePrefix(line)
			if !ok {
				continue
			}
			lines[i] = prefix + " " + targetImage + suffix
			replaced = true
			break
		}
	}

	return strings.Join(lines, "\n"), replaced
}

// updateRemoteK8sYamlImage 通过 SFTP 读取远程服务器上的独立 K8s YAML 文件，将其中的 image 修改为新构建的镜像:Tag 并持久化写回
func (r *pipelineRunner) updateRemoteK8sYamlImage(w io.Writer, targetImage string) error {
	yamlPath := r.k8sYamlFilePath()
	if yamlPath == "" || !isYamlPath(yamlPath) {
		return nil
	}

	if r.engine.dryRun() {
		fmt.Fprintf(w, "[dry-run] 📄 读取并修改远程 K8s YAML 文件: %s\n", yamlPath)
		fmt.Fprintf(w, "[dry-run] ✔ 已将 %s 中的 image 字段持久化更新为: %s\n", filepath.Base(yamlPath), targetImage)
		return nil
	}

	fmt.Fprintf(w, "📄 正在修改远程 K8s YAML 配置文件: %s ...\n", yamlPath)
	rawYaml, err := r.engine.SSH.ReadRemoteFile(r.srv, yamlPath)
	if err != nil {
		return fmt.Errorf("读取远程 YAML 文件 (%s) 失败: %w", yamlPath, err)
	}

	updatedYaml, ok := replaceYamlImage(rawYaml, r.app.Image, targetImage)
	if !ok {
		return fmt.Errorf("在远程 YAML 文件 (%s) 中未找到可替换的 image: 字段", yamlPath)
	}

	if err := r.engine.SSH.WriteRemoteFile(r.srv, yamlPath, updatedYaml); err != nil {
		return fmt.Errorf("写回远程 YAML 文件 (%s) 失败: %w", yamlPath, err)
	}

	fmt.Fprintf(w, "✔ 已将远程 YAML 文件 [%s] 中的镜像版本持久化修改为: %s\n", yamlPath, targetImage)
	return nil
}

func (r *pipelineRunner) renderCmd(tplStr string) (string, error) {
	t, err := template.New("cmd").Parse(tplStr)
	if err != nil {
		return "", err
	}
	yamlPath := r.k8sYamlFilePath()
	data := map[string]any{
		"LocalProjectDir":   r.app.ResolveLocalProjectDir(),
		"LocalArtifactPath": r.app.ResolveLocalArtifactPath(),
		"ArtifactDir":       r.app.ArtifactDir,
		"BuildDir":          r.app.BuildDir,
		"K8sYamlDir":        r.app.K8sYamlDir,
		"K8sYamlFile":       r.app.K8sYamlFile,
		"K8sYamlPath":       yamlPath,
		"Image":             r.app.Image,
		"Tag":               r.rel.Tag,
		"Namespace":         r.app.Namespace,
		"Deployment":        r.app.Deployment,
		"Container":         r.app.Container,
		"FileName":          r.rel.FileName,
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func runLocalCommand(ctx context.Context, workDir, cmdStr string, w io.Writer) error {
	var c *exec.Cmd
	if runtime.GOOS == "windows" {
		c = exec.CommandContext(ctx, "cmd", "/C", cmdStr)
	} else {
		c = exec.CommandContext(ctx, "sh", "-c", cmdStr)
	}
	if workDir != "" {
		c.Dir = workDir
	}
	c.Stdout = w
	c.Stderr = w
	return c.Run()
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

	// 0. 本地一键编译阶段（当开启本地编译时）
	if r.runLocalBuild {
		if err := r.stage("local_build", func(w io.Writer) error {
			cmd, err := r.renderCmd(r.app.LocalBuildCmd)
			if err != nil {
				return fmt.Errorf("渲染本地编译命令失败: %w", err)
			}
			projDir := r.app.ResolveLocalProjectDir()
			if projDir != "" {
				fmt.Fprintf(w, "📂 本地项目工作目录: %s\n", projDir)
			}
			fmt.Fprintf(w, "$ %s\n", cmd)

			// 若处于演练模式且本地项目目录不存在，则模拟本地编译过程
			if r.engine.dryRun() {
				if projDir == "" {
					time.Sleep(600 * time.Millisecond)
					fmt.Fprintf(w, "[dry-run] ✔ 本地编译完成，已生成产物: %s\n", r.artifact)
					return nil
				}
				if _, statErr := os.Stat(projDir); os.IsNotExist(statErr) {
					time.Sleep(600 * time.Millisecond)
					fmt.Fprintf(w, "[dry-run] ✔ 本地编译完成 (模拟)，目标产物: %s\n", r.artifact)
					return nil
				}
			}

			if err := runLocalCommand(r.ctx, projDir, cmd, w); err != nil {
				return fmt.Errorf("本地编译命令执行失败: %w", err)
			}

			name, sz, sum, err := fileStatAndSHA256(r.artifact)
			if err != nil {
				return fmt.Errorf("本地编译完成，但未找到编译产物 (%s): %w", r.artifact, err)
			}
			r.update(func(rel *store.Release) {
				rel.FileName = name
				rel.FileSize = sz
				rel.FileSHA256 = sum
			})
			r.rel.FileName = name
			r.rel.FileSize = sz
			r.rel.FileSHA256 = sum
			// 编译产物的文件名直接决定远程路径，统一套用与上传一致的白名单
			if err := upload.ValidateFileName(name); err != nil {
				return err
			}
			fmt.Fprintf(w, "✔ 本地编译成功! 产物: %s (%d bytes, SHA256: %s)\n", name, sz, sum[:12])
			return nil
		}); err != nil {
			return err
		}
	}

	// 1. 校验产物
	if err := r.stage("validate", func(w io.Writer) error {
		if r.engine.dryRun() {
			if _, statErr := os.Stat(r.artifact); os.IsNotExist(statErr) {
				fmt.Fprintf(w, "[dry-run] ✔ 产物文件 (%s) 校验通过 (模拟)\n", r.rel.FileName)
				return nil
			}
		}
		return validateArtifact(w, r.app.Type, r.artifact)
	}); err != nil {
		return err
	}

	// 2. 上传到服务器产物目录
	remoteArtifactPath := fmt.Sprintf("%s/%s", r.app.ArtifactDir, r.rel.FileName)
	if err := r.stage("upload", func(w io.Writer) error {
		if r.engine.dryRun() {
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
		syncCmd := fmt.Sprintf("mkdir -p %s && cp -f %s %s/%s", shellQuote(r.app.BuildDir), shellQuote(remoteArtifactPath), shellQuote(r.app.BuildDir), shellQuote(targetName))
		// go 二进制补齐可执行权限，避免用户 Dockerfile 只写 COPY 时镜像内无法启动
		if r.app.Type == "go" || r.app.Type == "binary" {
			syncCmd += fmt.Sprintf(" && chmod +x %s", shellQuote(r.app.BuildDir+"/"+targetName))
		}
		if r.engine.dryRun() {
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
		if r.engine.dryRun() {
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
			if r.engine.dryRun() {
				fmt.Fprintf(w, "[dry-run] $ %s\n", cmd)
				return nil
			}
			return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
		}); err != nil {
			return err
		}
	}

	// 6. 执行 K8s 部署（先修改远程独立 YAML 文件中的镜像版本并持久化保存，再执行 kubectl apply）
	if err := r.stage("deploy", func(w io.Writer) error {
		if err := r.updateRemoteK8sYamlImage(w, r.rel.Image); err != nil {
			return err
		}
		cmd, err := r.renderCmd(r.app.DeployCmd)
		if err != nil {
			return fmt.Errorf("渲染部署命令失败: %w", err)
		}
		if r.engine.dryRun() {
			fmt.Fprintf(w, "[dry-run] $ %s\n", cmd)
			return nil
		}
		return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
	}); err != nil {
		// 部署命令本身失败时，YAML 已持久化为新版本，同样需要回退，避免下次 apply 把坏版本推上去
		r.tryAutoRollback(err, "部署命令执行失败")
		return err
	}

	// 7. 等待 Pod 滚动就绪
	if err := r.stage("rollout", func(w io.Writer) error {
		return r.engine.Kube.RolloutStatus(r.ctx, w, r.srv, r.app.Namespace, r.app.Deployment, r.app.RolloutTimeout)
	}); err != nil {
		r.tryAutoRollback(err, "服务部署未就绪")
		return err
	}

	return nil
}

// tryAutoRollback 自动回滚：同时回退远程 YAML 文件中的镜像版本并重新 apply，保证 YAML 文件与线上一致。
// 用户主动取消时不回滚（尊重取消意图）；首次发布没有上一版本时仅提示，YAML 按下一阶段处理。
func (r *pipelineRunner) tryAutoRollback(cause error, reason string) {
	if !r.app.AutoRollback || r.rel.PrevImage == "" || r.rel.PrevImage == r.rel.Image {
		return
	}
	if !imageRefRe.MatchString(r.rel.PrevImage) {
		r.hub.Logf(fmt.Sprintf("\n[警告] 上一版本镜像引用不合法，跳过自动回滚: %q", r.rel.PrevImage))
		return
	}
	if r.ctx.Err() != nil || errors.Is(cause, context.Canceled) {
		r.hub.Logf("\n[警告] 发布已被取消，跳过自动回滚（YAML 与线上可能保留新版本，请确认）")
		return
	}
	// 非 YAML 部署（set image 模板）要求命名空间/Deployment/容器齐全，否则回滚命令必然失败
	if !isYamlPath(r.k8sYamlFilePath()) {
		if err := validateK8sTarget(r.app); err != nil {
			r.hub.Logf(fmt.Sprintf("\n[警告] 无法执行自动回滚：%v", err))
			return
		}
	}

	r.hub.Logf(fmt.Sprintf("\n[警告] %s，触发自动回滚到上一版本: %s", reason, r.rel.PrevImage))
	// 回滚使用独立 ctx，不受已失败流程的取消状态影响；但若用户此时主动取消，仍应尽快中止
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	go func() {
		select {
		case <-r.ctx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	var lastErr error
	if yamlPath := r.k8sYamlFilePath(); isYamlPath(yamlPath) {
		if err := r.updateRemoteK8sYamlImage(r.hub, r.rel.PrevImage); err != nil {
			lastErr = err
			r.hub.Logf("[错误] 回退远程 YAML 镜像版本失败: " + err.Error())
		}
		// 回滚 apply 必须带命名空间，与 DeployCmd 语义保持一致
		rbCmd := fmt.Sprintf("kubectl -n %s apply -f %s", shellQuote(r.app.Namespace), shellQuote(yamlPath))
		if r.engine.dryRun() {
			r.hub.Logf("[dry-run] $ " + rbCmd)
		} else if err := r.engine.SSH.RunCommand(ctx, r.srv, rbCmd, r.hub); err != nil {
			lastErr = err
			r.hub.Logf("[错误] 回滚 apply 失败: " + err.Error())
		}
	} else if err := r.engine.Kube.SetImage(ctx, r.hub, r.srv, r.app.Namespace, r.app.Deployment, r.app.Container, r.rel.PrevImage); err != nil {
		lastErr = err
		r.hub.Logf("[错误] 回滚 set image 失败: " + err.Error())
	}

	if lastErr == nil {
		r.update(func(rel *store.Release) {
			rel.Status = store.StatusRolledBack
		})
		r.hub.Logf("✔ 已自动回滚至上一版本，配置与线上保持一致")
	} else {
		r.hub.Logf("[错误] 自动回滚未能完成，请手动确认线上版本与 YAML 文件")
	}
}

func (r *pipelineRunner) runRollback() error {
	r.hub.Logf(fmt.Sprintf("==> 🚨 开始回滚应用: %s [目标版本: %s]", r.app.Name, r.rel.Tag))
	r.hub.Logf(fmt.Sprintf("==> 目标镜像: %s", r.rel.Image))
	if r.rel.RollbackOf != "" {
		r.hub.Logf(fmt.Sprintf("==> 关联历史版本记录: %s", r.rel.RollbackOf))
	}

	// 1. 修改远程 YAML 镜像版本并执行部署命令
	if err := r.stage("deploy", func(w io.Writer) error {
		if err := r.updateRemoteK8sYamlImage(w, r.rel.Image); err != nil {
			return err
		}
		cmd, err := r.renderCmd(r.app.DeployCmd)
		if err != nil {
			return fmt.Errorf("渲染部署命令失败: %w", err)
		}
		if r.engine.dryRun() {
			fmt.Fprintf(w, "[dry-run] $ %s\n", cmd)
			return nil
		}
		return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
	}); err != nil {
		return err
	}

	// 2. 等待 Pod 就绪
	if err := r.stage("rollout", func(w io.Writer) error {
		return r.engine.Kube.RolloutStatus(r.ctx, w, r.srv, r.app.Namespace, r.app.Deployment, r.app.RolloutTimeout)
	}); err != nil {
		return err
	}

	return nil
}

func (r *pipelineRunner) finish(err error) {
	now := time.Now()
	status := store.StatusSuccess
	if err != nil {
		// 阶段内已自行处理过的终态（如自动回滚成功）不应在结尾被覆盖
		if errors.Is(err, context.Canceled) {
			status = store.StatusCanceled
		} else {
			status = store.StatusFailed
		}
	}
	r.update(func(rel *store.Release) {
		// 自动回滚成功时 tryAutoRollback 已把状态标为 rolledback：普通失败保留该终态，取消则如实记录
		if rel.Status != store.StatusRolledBack || err == nil || errors.Is(err, context.Canceled) {
			rel.Status = status
		}
		rel.FinishedAt = &now
		if err != nil {
			rel.Error = err.Error()
		}
	})
	if status == store.StatusSuccess {
		if r.rel.Kind == "rollback" {
			r.hub.Logf(fmt.Sprintf("\n==> 🎉 回滚成功! 已回退至版本: %s (%s)", r.rel.Tag, r.rel.Image))
		} else {
			r.hub.Logf(fmt.Sprintf("\n==> 🎉 发布成功! 镜像已上线: %s", r.rel.Image))
			// 自动执行历史版本与磁盘清理
			r.cleanup()
		}
	} else {
		r.hub.Logf(fmt.Sprintf("\n==> ❌ 发布终止: %v", err))
	}
}

func (r *pipelineRunner) cleanup() {
	r.hub.Logf("\n==> 阶段 [cleanup]: 自动清理历史版本产物与回收镜像缓存")
	keepCount := 5 // 默认保留最近 5 个产物版本
	if r.app.ArtifactDir != "" && r.app.ArtifactDir != "/" {
		// 只清理本应用历次上传的产物文件，避免误删目录内手工放置的 Dockerfile、配置等
		seen := map[string]bool{}
		var artifactNames []string
		for _, rel := range r.engine.Store.List(r.app.ID, 0) {
			if rel.FileName != "" && upload.ValidFileName(rel.FileName) && !seen[rel.FileName] {
				seen[rel.FileName] = true
				artifactNames = append(artifactNames, shellQuote(r.app.ArtifactDir+"/"+rel.FileName))
			}
		}
		if len(artifactNames) > keepCount {
			// tail 按输入顺序保留最后 keepCount 个（Store.List 已按时间倒序，即最近的），不使用 ls -t 排序
			cleanCmd := fmt.Sprintf("printf '%%s\\n' %s | tail -n +%d | while IFS= read -r f; do [ -n \"$f\" ] && rm -f -- \"$f\"; done", strings.Join(artifactNames, " "), keepCount+1)
			if r.engine.dryRun() {
				r.hub.Logf(fmt.Sprintf("[dry-run] 模拟清理远程产物: $ %s", cleanCmd))
			} else {
				_ = r.engine.SSH.RunCommand(context.Background(), r.srv, cleanCmd, io.Discard)
				r.hub.Logf(fmt.Sprintf("✔ 已清理远程产物目录 %s，保留最近 %d 个版本包", r.app.ArtifactDir, keepCount))
			}
		}
	}

	// 清理本次上传产生的本地 .part 副本（上传目录只是缓存）
	uploadsDir := filepath.Join(r.engine.DataDir, "uploads") + string(os.PathSeparator)
	if strings.HasPrefix(r.artifact, uploadsDir) {
		_ = os.Remove(r.artifact)
	}

	// 自动回收 Docker 悬空镜像 (dangling images)
	if !r.engine.dryRun() {
		_ = r.engine.SSH.RunCommand(context.Background(), r.srv, "docker image prune -f", io.Discard)
		r.hub.Logf("✔ 已回收远程 Docker 悬空虚悬镜像 (dangling images)")
	}
}

func (e *Engine) Hub(id string) *Hub {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.hubs[id]
}

// dropHub 发布结束后移除 Hub 引用；已在途的 SSE 订阅持有自己的指针，不受影响
func (e *Engine) dropHub(id string) {
	e.mu.Lock()
	delete(e.hubs, id)
	e.mu.Unlock()
}

func (e *Engine) Cancel(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range e.running {
		if r.releaseID == id {
			if r.cancel == nil {
				return errors.New("该发布尚未开始执行，请稍候")
			}
			r.cancel()
			return nil
		}
	}
	return errors.New("该发布未在运行")
}

// validateAppForPipeline 发布与回滚共用的配置校验：
// 模板变量会直接渲染进远程 shell 命令，手改 YAML 或旧配置可能绕过保存时的校验
func validateAppForPipeline(app *config.AppConfig) error {
	if err := config.ValidateApp(app); err != nil {
		return fmt.Errorf("应用配置不合法: %w", err)
	}
	if strings.TrimSpace(app.Image) == "" {
		return errors.New("应用未配置镜像名称 (Image)")
	}
	return nil
}

// validateK8sTarget 校验 set image 回滚所需的命名空间/Deployment/容器是否齐全且合法
func validateK8sTarget(app *config.AppConfig) error {
	if app.Namespace == "" || app.Deployment == "" || app.Container == "" {
		return errors.New("应用未配置完整的命名空间 / Deployment / 容器名称")
	}
	return config.ValidateApp(app)
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
