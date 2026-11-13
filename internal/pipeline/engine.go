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

	"build-to-deploy/internal/buildstage"
	"build-to-deploy/internal/config"
	"build-to-deploy/internal/dockerrun"
	"build-to-deploy/internal/k8s"
	"build-to-deploy/internal/shellrun"
	"build-to-deploy/internal/sshpool"
	"build-to-deploy/internal/store"
	"build-to-deploy/internal/templates"
	"build-to-deploy/internal/upload"
)

type Engine struct {
	Cfg     *config.Config
	Store   *store.Store
	Uploads *upload.Manager
	SSH     shellrun.Runner
	Kube    *k8s.Client
	DataDir string

	mu      sync.Mutex
	running map[string]*activeRun // appID -> activeRun
	hubs    map[string]*Hub       // releaseID -> Hub
	fpCache map[string]*fingerprintValue
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
		fpCache: make(map[string]*fingerprintValue),
	}
	_ = os.MkdirAll(e.logDir(), 0755)
	return e
}

func (e *Engine) logDir() string           { return filepath.Join(e.DataDir, "logs") }
func (e *Engine) LogPath(id string) string { return filepath.Join(e.logDir(), id+".log") }

type Conflict struct {
	Source  string `json:"source"` // docker | k8s | fingerprint | k8s-image
	Message string `json:"message"`
	Kind    string `json:"kind,omitempty"` // image：构建后确认与线上/已部署镜像是同一镜像（部署阶段可复用，不重建）
}

type PrecheckResult struct {
	Conflicts  []Conflict `json:"conflicts"`
	Warnings   []string   `json:"warnings"`
	Current    string     `json:"current"`
	CurrentTag string     `json:"currentTag"`
	// CurrentImage 线上镜像的内容摘要（sha256 摘要或本地镜像 ID）：部署阶段据此判断能否直接复用镜像
	CurrentImage string `json:"currentImage,omitempty"`
	ImageFp      string `json:"imageFp,omitempty"` // 本次发布的构建输入指纹（与 Tag 无关）
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

	if app.DeployMode == config.DeployModeDocker {
		e.precheckDocker(ctx, srv, app, tag, ref, res)
		return res, nil
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
		// 新 Tag，但线上镜像的内容摘要已经上线过：换了名字的同一个镜像
		if !strings.HasPrefix(res.CurrentTag, "sha256") {
			res.CurrentImage = e.currentImageSummary(ctx, srv, app)
		}
		if img := res.CurrentImage; img != "" {
			if rels := e.Store.FindByImage(app.ID, img); len(rels) > 0 && rels[0].Tag != tag {
				last := rels[0]
				res.Conflicts = append(res.Conflicts, Conflict{
					Source:  "k8s-image",
					Kind:    "image",
					Message: fmt.Sprintf("线上当前运行的镜像内容 (摘要 %s) 与历史版本 %s 完全相同，Tag 不同但镜像是同一个", img, last.Tag),
				})
			}
		}
	} else if err != nil {
		res.Warnings = append(res.Warnings, "获取 K8s 当前镜像状态跳过: "+err.Error())
	}

	return res, nil
}

// precheckDocker 容器部署形态的预检：看宿主机上同名容器在不在、跑的是哪个镜像
func (e *Engine) precheckDocker(ctx context.Context, srv *sshpool.ServerConfig, app *config.AppConfig, tag, ref string, res *PrecheckResult) {
	insp, err := dockerrun.InspectCmdFor(app)
	if err != nil {
		res.Warnings = append(res.Warnings, "Docker 容器配置不完整，跳过容器状态检查: "+err.Error())
		return
	}
	out, err := e.SSH.ExecuteOutput(ctx, srv, insp)
	if err != nil {
		res.Warnings = append(res.Warnings, "获取容器状态跳过: "+err.Error())
		return
	}
	status, image := dockerrun.ParseInspect(out)
	if image == "" {
		return
	}
	res.Current = image
	res.CurrentTag = extractTag(image)
	if res.CurrentTag == tag {
		res.Conflicts = append(res.Conflicts, Conflict{
			Source:  "docker",
			Message: fmt.Sprintf("宿主机容器 %s 当前正在运行此版本 (%s，状态: %s)", app.DockerContainer, ref, status),
		})
	}
	if strings.HasPrefix(image, "sha256:") {
		res.CurrentImage = image
	} else {
		res.CurrentImage = e.imageSummary(ctx, srv, image)
	}
	if img := res.CurrentImage; img != "" {
		if rels := e.Store.FindByImage(app.ID, img); len(rels) > 0 && rels[0].Tag != tag {
			res.Conflicts = append(res.Conflicts, Conflict{
				Source:  "docker-image",
				Kind:    "image",
				Message: fmt.Sprintf("宿主机容器里的镜像内容 (摘要 %s) 与历史版本 %s 完全相同，Tag 不同但镜像是同一个", img, rels[0].Tag),
			})
		}
	}
}

// imageSummary 查询任意镜像引用的内容摘要（RepoDigest 优先，退回镜像 ID）
func (e *Engine) imageSummary(ctx context.Context, srv *sshpool.ServerConfig, ref string) string {
	if !imageRefRe.MatchString(ref) {
		return ""
	}
	cmd := fmt.Sprintf("docker image inspect -f %s %s 2>/dev/null",
		shellQuote(`{{.Id}} {{json .RepoDigests}}`), shellQuote(ref))
	out, err := e.SSH.ExecuteOutput(ctx, srv, cmd)
	if err != nil {
		return ""
	}
	return pickDigest(buildstage.SummarizeImages(out, imageRepoOf(ref), extractTag(ref)))
}

// currentImageSummary 返回线上容器镜像的内容摘要：优先取 RepoDigests 中的 sha256 摘要，失败时退回镜像 ID。
// 用途是把"换了个名字的同一镜像"识别出来，因此必须以内容摘要为准，不能比较 Tag。
func (e *Engine) currentImageSummary(ctx context.Context, srv *sshpool.ServerConfig, app *config.AppConfig) string {
	fmtSel := shellQuote(`{{.Id}} {{json .RepoDigests}}`)
	cmd := fmt.Sprintf("kubectl -n %s get deployment %s -o jsonpath=%s | xargs -r docker image inspect -f %s 2>/dev/null",
		shellQuote(app.Namespace), shellQuote(app.Deployment),
		shellQuote(fmt.Sprintf(`{.spec.template.spec.containers[?(@.name=="%s")].image}`, app.Container)), fmtSel)
	out, err := e.SSH.ExecuteOutput(ctx, srv, cmd)
	if err != nil {
		return ""
	}
	repo := app.Image
	for _, s := range buildstage.SummarizeImages(out, repo, "") {
		if strings.HasPrefix(s, "sha256:") && len(s) == len("sha256:")+64 {
			return s
		}
	}
	// 没有 RepoDigests（本地构建、未推仓库）时用镜像 ID 兜底：本地同名镜像的 ID 与内容一一对应
	for _, s := range buildstage.SummarizeImages(out, repo, "") {
		return s
	}
	return ""
}

// FingerprintSkipReason 说明为什么无法计算指纹，供前端提示（不阻断发布）
func (e *Engine) FingerprintSkipReason(app *config.AppConfig, src ArtifactSource) string {
	if _, err := renderCmdTemplate(app.BuildCmd, app, "t"); err != nil {
		return "构建命令渲染失败: " + err.Error()
	}
	if e.buildSpec(app, src.Tag) == nil {
		return "构建命令无法静态解析（构建目录或 Dockerfile 不清，如构建目录为相对路径、Dockerfile 不在构建目录内）"
	}
	if src.LocalArtifact != "" {
		if _, err := os.Stat(src.LocalArtifact); err != nil {
			return "找不到本地产物: " + src.LocalArtifact
		}
		return "产物摘要为空"
	}
	return "未选择产物"
}

// ArtifactFingerprint 预检结果：构建输入指纹与产物摘要
type ArtifactFingerprint struct {
	Fingerprint string // 构建输入指纹，空表示无法静态判断（构建命令解析失败或缺少产物）
	SHA256      string // 产物内容摘要
	Skipped     int    // 指纹未覆盖的输入数量（通配符、--build-arg 等）
	TagFree     bool   // 构建命令不引用 Tag：换任何 Tag 构建出的镜像内容都相同
}

// InspectArtifact 在发布前（尚未上传时）计算构建输入指纹，用来确认新 Tag 下是否与历史版本是同一个镜像。
// 上传完成后调用方用 Completed 传入分片摘要，避免重复读一遍 150MB 的产物。
func (e *Engine) InspectArtifact(app *config.AppConfig, src ArtifactSource) (*ArtifactFingerprint, error) {
	spec := e.buildSpec(app, src.Tag)
	if spec == nil {
		fmt.Fprintln(os.Stderr, "DEBUG nil buildCmd=", app.BuildCmd, " resolved=", app.ResolveLocalArtifactPath())
		return &ArtifactFingerprint{}, nil
	}
	sha := ""
	switch {
	case src.Completed != nil:
		if !src.Completed.Done {
			return nil, fmt.Errorf("上传任务 %s 尚未完成，请等待完成后再预检", src.Completed.ID)
		}
		sha = src.Completed.SHA256
	case src.UploadID != "":
		up, err := e.Uploads.Get(src.UploadID)
		if err != nil {
			return nil, err
		}
		if !up.Done {
			return nil, fmt.Errorf("上传任务 %s 尚未完成（已接收 %d/%d），请等待完成后再预检", src.UploadID, up.Received, up.Size)
		}
		sha = up.SHA256
	case src.LocalArtifact != "":
		_, _, sum, err := fileStatAndSHA256(src.LocalArtifact)
		if err != nil {
			return nil, fmt.Errorf("读取本地产物失败 (%s): %w", src.LocalArtifact, err)
		}
		sha = sum
	}
	if sha == "" {
		return &ArtifactFingerprint{}, nil
	}
	fp, skipped, err := e.fingerprintSpec(spec, sha)
	if err != nil {
		return nil, err
	}
	return &ArtifactFingerprint{Fingerprint: fp, SHA256: sha, Skipped: skipped, TagFree: !spec.Tagged}, nil
}

// appFingerprintKey 懒加载缓存键：覆盖"构建目录 + Dockerfile + 产物摘要"
func appFingerprintKey(spec *buildstage.Spec, artifactSHA string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s|%s|%s|%v", spec.Dir, spec.Dockerfile, artifactSHA, spec.Tagged)
	for _, in := range spec.Inputs {
		b.WriteString("|")
		b.WriteString(in)
	}
	return b.String()
}

// ArtifactSource 预检时的产物来源：已上传的分片、上传任务，或本地文件
type ArtifactSource struct {
	Tag           string
	UploadID      string
	Completed     *upload.Upload
	LocalArtifact string
}

// fingerprintSpec 计算构建输入指纹；构建命令与 Tag 无关时命中缓存直接返回，避免重复读构建上下文
func (e *Engine) fingerprintSpec(spec *buildstage.Spec, artifactSHA string) (fp string, skipped int, err error) {
	if artifactSHA == "" {
		return "", 0, nil
	}
	key := appFingerprintKey(spec, artifactSHA)
	if !spec.Tagged {
		e.mu.Lock()
		if kv, ok := e.fpCache[key]; ok {
			e.mu.Unlock()
			return kv.fp, kv.skipped, nil
		}
		e.mu.Unlock()
	}
	fp, skipped = spec.Fingerprint(artifactSHA)
	if !spec.Tagged {
		e.mu.Lock()
		e.fpCache[key] = &fingerprintValue{fp: fp, skipped: skipped}
		e.mu.Unlock()
	}
	return fp, skipped, nil
}

type fingerprintValue struct {
	fp      string
	skipped int
}

// buildSpec 解析构建命令的构建要素；命令里未写 {{.Tag}}（或解析失败）返回 nil。
// 注意：Tag 必须代入真实值（如 "v1"）再解析，空 Tag 会让 docker build 的 -t 参数没有值，解析整体失败。
func (e *Engine) buildSpec(app *config.AppConfig, tag string) *buildstage.Spec {
	if tag == "" {
		tag = "t"
	}
	cmd, err := renderCmdTemplate(app.BuildCmd, app, tag)
	if err != nil {
		return nil
	}
	return buildstage.Parse(cmd)
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
	// NoReuse 用户选择"即使内容相同也重新构建"：跳过复用已有镜像，强制执行 docker build
	NoReuse bool
	// BuildDir / BuildCmd 前端未保存的构建配置草稿，非空时覆盖已保存配置
	BuildDir string
	BuildCmd string
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
	return e.startDeploy(context.Background(), req)
}

// StartDeployContext 与 StartDeploy 相同，但启动前阶段（预检、读产物）可随调用方 ctx 取消（HTTP 请求断开时由 server 传入请求 ctx）。
// 注意：流水线本身不继承该 ctx——HTTP handler 写出响应后请求 ctx 即被取消，
// 若沿用会让流水线刚启动就 context canceled；运行中取消一律走 Cancel 接口。
func (e *Engine) StartDeployContext(parent context.Context, req StartRequest) (*store.Release, error) {
	return e.startDeploy(parent, req)
}

func (e *Engine) startDeploy(parent context.Context, req StartRequest) (*store.Release, error) {
	app := e.Cfg.FindApp(req.AppID)
	if app == nil {
		return nil, fmt.Errorf("应用 %s 不存在", req.AppID)
	}
	// 前端携带的构建目录/构建命令优先（用户在"修改编译配置"里改了但还没保存时，按页面上的值如实发布）
	app = app.WithOverrides(req.BuildDir, req.BuildCmd)
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
	// 流水线 ctx 不继承请求 ctx（见函数注释），只在 Cancel / 正常结束时取消
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

	// 预检的超时比远端操作宽松；启动前阶段跟随请求 ctx，请求断开即放弃
	pctx, pcancel := context.WithTimeout(parent, 60*time.Second)
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
			return nil, fmt.Errorf("读取本地默认产物失败 (%s): %w", localArtifact, err)
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
	switch app.DeployMode {
	case config.DeployModeDocker:
		stages = append(stages,
			store.Stage{Name: "deploy", Title: "启动 Docker 容器", Status: store.StatusPending},
			store.Stage{Name: "rollout", Title: "等待容器就绪", Status: store.StatusPending},
		)
	default:
		stages = append(stages,
			store.Stage{Name: "deploy", Title: "执行 K8s 部署", Status: store.StatusPending},
			store.Stage{Name: "rollout", Title: "等待 Pod 就绪", Status: store.StatusPending},
		)
	}

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
		Mode:       app.DeployMode,
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
			noReuse:       req.NoReuse,
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
	// 平台不再自动删除旧镜像，但服务器上的镜像可能被手工清理或被 docker 回收：
	// 启动回滚前先确认目标镜像仍在，避免 rollback 跑到一半才失败、留下半套记录。
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 30*time.Second)
	missing, checkErr := e.checkImageExists(checkCtx, srv, target.Image)
	checkCancel()
	if checkErr != nil {
		return nil, fmt.Errorf("确认目标镜像 %s 是否存在失败: %w", target.Image, checkErr)
	}
	if missing {
		return nil, fmt.Errorf("目标镜像 %s 已不在服务器 %s 上，无法回滚；请先在服务器上恢复该镜像或重新发布该版本", target.Image, srv.Host)
	}

	stages := []store.Stage{
		{Name: "deploy", Title: "执行 K8s 镜像更新", Status: store.StatusPending},
		{Name: "rollout", Title: "等待 Pod 滚动就绪", Status: store.StatusPending},
	}
	if app.DeployMode == config.DeployModeDocker {
		stages = []store.Stage{
			{Name: "deploy", Title: "回滚 Docker 容器", Status: store.StatusPending},
			{Name: "rollout", Title: "等待容器就绪", Status: store.StatusPending},
		}
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
		Mode:       app.DeployMode,
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
	noReuse       bool // 用户明确要求重新构建：不复用内容相同的已有镜像
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

func (r *pipelineRunner) k8sYamlFilePath() string { return k8sYamlFilePath(r.app) }

func k8sYamlFilePath(app *config.AppConfig) string {
	yamlPath := strings.TrimSpace(app.K8sYamlDir)
	if app.K8sYamlFile != "" {
		if yamlPath != "" {
			yamlPath = strings.TrimRight(yamlPath, "/") + "/" + strings.TrimLeft(app.K8sYamlFile, "/")
		} else {
			yamlPath = app.K8sYamlFile
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
	return renderCmdTemplate(tplStr, r.app, r.rel.Tag, r.rel.FileName)
}

// cmdData 命令模板可用变量（发布与预检共用，保证两边渲染结果一致）
func cmdData(app *config.AppConfig, tag, fileName string) map[string]any {
	yamlPath := k8sYamlFilePath(app)
	return map[string]any{
		"LocalProjectDir":   app.ResolveLocalProjectDir(),
		"LocalArtifactPath": app.ResolveLocalArtifactPath(),
		"ArtifactDir":       app.ArtifactDir,
		"BuildDir":          app.BuildDir,
		"K8sYamlDir":        app.K8sYamlDir,
		"K8sYamlFile":       app.K8sYamlFile,
		"K8sYamlPath":       yamlPath,
		"Image":             app.Image,
		"Tag":               tag,
		"Namespace":         app.Namespace,
		"Deployment":        app.Deployment,
		"Container":         app.Container,
		"FileName":          fileName,
	}
}

func renderCmdTemplate(tplStr string, app *config.AppConfig, tag string, fileName ...string) (string, error) {
	t, err := template.New("cmd").Parse(tplStr)
	if err != nil {
		return "", err
	}
	name := ""
	if len(fileName) > 0 {
		name = fileName[0]
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, cmdData(app, tag, name)); err != nil {
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
	if r.app.DeployMode == config.DeployModeDocker {
		r.hub.Logf(fmt.Sprintf("==> 部署形态: Docker 容器 (%s)", r.app.DockerContainer))
	} else if r.app.K8sYamlFile != "" {
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

	// 1. 校验产物并计算构建输入指纹（产物内容 + 构建上下文）：与历史版本相同即"换了 Tag 的同一个镜像"
	if err := r.stage("validate", func(w io.Writer) error {
		if err := validateArtifact(w, r.app.Type, r.artifact); err != nil {
			return err
		}
		spec := r.buildSpec(w)
		if spec == nil {
			return nil
		}
		if r.rel.FileSHA256 == "" {
			fmt.Fprintln(w, "[警告] 产物摘要未知，跳过镜像内容重复检测")
			return nil
		}
		fp, skipped, err := r.engine.fingerprintSpec(spec, r.rel.FileSHA256)
		if err != nil {
			return err
		}
		r.update(func(rel *store.Release) { rel.ImageFP = fp })
		r.rel.ImageFP = fp
		fmt.Fprintf(w, "构建输入指纹: %s (产物 SHA256: %s)\n", buildstage.Short(fp, 12), buildstage.Short(r.rel.FileSHA256, 12))
		if skipped > 0 {
			fmt.Fprintf(w, "[警告] 构建命令中有 %d 处输入无法静态解析（通配符 / --build-arg 等），镜像内容比对可能不完整\n", skipped)
		}
		if prev := r.engine.Store.FindFingerprint(r.app.ID, fp); prev != nil {
			fmt.Fprintf(w, "[警告] 本次产物与构建上下文和已发布版本 %s 完全相同：构建出的镜像内容与它一致，本 Tag 只是同一个镜像的另一个名字\n", prev.Tag)
		} else if !spec.Tagged {
			fmt.Fprintln(w, "[警告] 构建命令不引用 {{.Tag}}，本次与任何 Tag 构建出的镜像内容都相同")
		}
		return nil
	}); err != nil {
		return err
	}

	// 2. 上传到服务器产物目录
	remoteArtifactPath := fmt.Sprintf("%s/%s", r.app.ArtifactDir, r.rel.FileName)
	if err := r.stage("upload", func(w io.Writer) error {
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
		return r.engine.SSH.RunCommand(r.ctx, r.srv, syncCmd, w)
	}); err != nil {
		return err
	}

	// 4. 执行镜像构建（或复用内容相同的已有镜像）
	if err := r.stage("build", func(w io.Writer) error {
		if r.noReuse {
			fmt.Fprintln(w, "已选择重新构建：跳过复用已有镜像，直接执行 docker build")
			if err := r.buildImage(w); err != nil {
				return err
			}
			if err := r.labelImage(w); err != nil {
				fmt.Fprintf(w, "[警告] 写入镜像指纹标签失败（不影响发布）: %v\n", err)
			}
			return r.checkBuiltImage(w)
		}
		reuse, full, err := r.findReusableImage(w)
		if err != nil {
			return err
		}
		if reuse != "" && full != "" {
			// 前提：复用镜像与内容相同的历史镜像打上本 Tag。只做 RENAME，产物与构建上下文由指纹保证一致，
			// 因此不会把"构建命令本身会失败"的情况静默放过去
			fmt.Fprintf(w, "✔ 已找到内容相同的镜像 %s（摘要 %s，由历史版本构建，构建输入指纹与本次一致）\n", reuse, buildstage.Short(full, 19))
			fmt.Fprintf(w, "复用构建结果，不重复执行 docker build，仅追加 Tag: %s\n", r.rel.Image)
			if err := r.retagImage(w, reuse, full); err != nil {
				fmt.Fprintf(w, "[警告] 为内容相同的镜像追加 Tag 失败，将退回正常构建: %v\n", err)
				return r.buildImage(w)
			}
			r.update(func(rel *store.Release) { rel.ImageSummary = full })
			r.rel.ImageSummary = full
			return nil
		}

		if err := r.buildImage(w); err != nil {
			return err
		}
		if err := r.labelImage(w); err != nil {
			fmt.Fprintf(w, "[警告] 写入镜像指纹标签失败（不影响发布）: %v\n", err)
		}
		// 摘要已在 labelImage 中取得并写入记录；内容与线上完全相同的情况在这里拒绝部署
		return r.checkBuiltImage(w)
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
			return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
		}); err != nil {
			return err
		}
	}

	// 6. 执行部署：K8s 形态改写 YAML 后 kubectl apply；Docker 形态直接 docker rm/run 单容器
	if r.app.DeployMode == config.DeployModeDocker {
		if err := r.stage("deploy", func(w io.Writer) error {
			return r.deployDocker(w, r.rel.Image)
		}); err != nil {
			r.tryAutoRollback(err, "容器启动命令执行失败")
			return err
		}
		// 7. 等待容器进入 running 并（可选）健康检查通过
		if err := r.stage("rollout", func(w io.Writer) error {
			return r.awaitDocker(w)
		}); err != nil {
			r.tryAutoRollback(err, "容器未就绪")
			return err
		}
		return nil
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

// buildSpec 渲染构建命令并解析构建要素；无法解析时在日志中说明，返回 nil 让流水线照常构建
func (r *pipelineRunner) buildSpec(w io.Writer) *buildstage.Spec {
	spec := r.engine.buildSpec(r.app, r.rel.Tag)
	if spec == nil {
		fmt.Fprintln(w, "[提示] 构建命令无法静态解析，跳过镜像内容重复检测")
	}
	return spec
}

// buildImage 执行渲染后的构建命令
func (r *pipelineRunner) buildImage(w io.Writer) error {
	cmd, err := r.renderCmd(r.app.BuildCmd)
	if err != nil {
		return fmt.Errorf("渲染构建命令失败: %w", err)
	}
	return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
}

// inspectImage 读取镜像的内容摘要：优先 RepoDigests 中的 sha256 摘要，未推仓库时退回镜像 ID
func (r *pipelineRunner) inspectImage(ref string) string {
	cmd := fmt.Sprintf("docker image inspect -f %s %s 2>/dev/null",
		shellQuote(`{{.Id}} {{json .RepoDigests}}`), shellQuote(ref))
	out, err := r.engine.SSH.ExecuteOutput(r.ctx, r.srv, cmd)
	if err != nil {
		return ""
	}
	return pickDigest(buildstage.SummarizeImages(out, imageRepoOf(ref), extractTag(ref)))
}

// pickDigest 优先返回完整 sha256 摘要（64 位），否则返回第一个值（本地镜像 ID）
func pickDigest(list []string) string {
	for _, s := range list {
		if strings.HasPrefix(s, "sha256:") && len(s) == len("sha256:")+64 {
			return s
		}
	}
	if len(list) > 0 {
		return list[0]
	}
	return ""
}

// recordImageSummary 记录构建后镜像的内容摘要，供下次发布比对
func (r *pipelineRunner) recordImageSummary(w io.Writer, ref string) error {
	sum := r.inspectImage(ref)
	if sum == "" {
		return nil
	}
	r.update(func(rel *store.Release) { rel.ImageSummary = sum })
	r.rel.ImageSummary = sum
	fmt.Fprintf(w, "镜像内容摘要: %s\n", sum)
	return nil
}

// findReusableImage 查找内容相同的已有镜像。
// 顺序：K8s 线上正在运行的镜像 -> 本应用历史发布记录里的镜像 -> 服务器上打过相同指纹标签的镜像
// （包含其他应用、手工 docker build 的场景）。返回可复用的镜像名与摘要；找不到时由调用方正常构建。
func (r *pipelineRunner) findReusableImage(w io.Writer) (string, string, error) {
	fp := r.rel.ImageFP
	if fp == "" {
		return "", "", nil
	}

	// 线上正在运行的镜像本身就是当前版本时，复用它等于不部署：交给正常构建，由构建后比对给出明确提示
	runningIsTarget := r.precheck.CurrentTag == r.rel.Tag
	if cur := r.precheck.CurrentImage; cur != "" && !runningIsTarget {
		fmt.Fprintf(w, "比对线上镜像 %s 的内容摘要...\n", cur)
		for _, ref := range r.refsForImage(cur) {
			if sum := r.inspectImage(ref); sum != "" && sum == cur {
				return ref, sum, nil
			}
		}
	}

	fpLabel := buildstage.Short(r.fingerprintKey(fp), 26)
	// 历史记录只存摘要，不知道指纹是否随 Tag 变化，这里逐个与当前指纹标签核对：
	// 能对上的镜像内容一定相同（同一构建输入 + 同一镜像），对不上的继续往下找
	for _, rel := range r.engine.Store.List(r.app.ID, 0) {
		if rel.ID == r.rel.ID || rel.ImageSummary == "" || !config.TagRe.MatchString(rel.Tag) {
			continue
		}
		if rel.ImageSummary == r.rel.PrevImage {
			// 线上正在运行的镜像：复用并指向它 = 本次部署不会产生任何变化
			continue
		}
		if rel.ImageFP != "" && buildstage.Short(r.fingerprintKey(rel.ImageFP), 26) != fpLabel {
			continue
		}
		ref := fmt.Sprintf("%s:%s", r.app.Image, rel.Tag)
		if r.inspectImage(ref) == rel.ImageSummary {
			return ref, rel.ImageSummary, nil
		}
	}

	listCmd := fmt.Sprintf("docker images --no-trunc --format %s", shellQuote(`{{.Repository}}:{{.Tag}} {{.ID}}`))
	out, err := r.engine.SSH.ExecuteOutput(r.ctx, r.srv, listCmd)
	if err != nil {
		fmt.Fprintf(w, "[警告] 读取服务器镜像列表失败，忽略镜像复用: %v\n", err)
		return "", "", nil
	}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || strings.Contains(line, "<none>") {
			continue
		}
		ref := fields[0]
		label, err := r.engine.SSH.ExecuteOutput(r.ctx, r.srv,
			fmt.Sprintf("docker image inspect -f %s %s 2>/dev/null", shellQuote(`{{index .Config.Labels "build-to-deploy.fingerprint"}}`), shellQuote(ref)))
		if err != nil || strings.TrimSpace(label) != fpLabel {
			continue
		}
		sum := r.inspectImage(ref)
		if sum == "" {
			continue
		}
		return ref, sum, nil
	}
	return "", "", nil
}

// refsForImage 把线上镜像引用（Tag 或 sha256 摘要）换算成可以 inspect / 传给 docker tag 的镜像引用
func (r *pipelineRunner) refsForImage(cur string) []string {
	if strings.HasPrefix(cur, "sha256:") {
		// 摘要（RepoDigest）需要连同仓库名一起使用
		return []string{r.app.Image + "@" + cur}
	}
	return []string{cur}
}

// checkImageExists 确认镜像引用在服务器上仍然存在；missing 为 true 表示已不存在。
// 用 docker image inspect 而非 docker images：前者对 tag、摘要（repo@sha256:...）都能识别。
func (e *Engine) checkImageExists(ctx context.Context, srv *sshpool.ServerConfig, ref string) (missing bool, err error) {
	if !imageRefRe.MatchString(ref) {
		return false, fmt.Errorf("镜像引用不合法: %q", ref)
	}
	cmd := fmt.Sprintf("docker image inspect -f %s %s >/dev/null 2>&1; echo $?", shellQuote("{{.Id}}"), shellQuote(ref))
	out, err := e.SSH.ExecuteOutput(ctx, srv, cmd)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "0", nil
}

// fingerprintKey 标签里存的指纹：只保留前 26 位，足够区分不同构建输入
func (r *pipelineRunner) fingerprintKey(fp string) string {
	return fp
}

// retagImage 为内容相同的镜像追加本 Tag；失败返回错误由调用方决定是否退回构建
func (r *pipelineRunner) retagImage(w io.Writer, src, sum string) error {
	cmd := fmt.Sprintf("docker tag %s %s", shellQuote(src), shellQuote(r.rel.Image))
	return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
}

// labelImage 给构建出的镜像打一个指纹标签，供下次发布在"构建前"识别内容相同的镜像。
// 做法：以本镜像的摘要为起点派生一个只含 LABEL 的探针镜像再打上固定标签名——
// 重新构建同一镜像是幂等的（摘要不变），不会因反复构建而堆积镜像。
// 该命令由本机 /bin/sh 执行（不依赖容器内工具），任何一步失败只告警，不影响发布。
func (r *pipelineRunner) labelImage(w io.Writer) error {
	if r.rel.ImageFP == "" {
		return nil
	}
	sum := r.inspectImage(r.rel.Image)
	if sum == "" {
		return nil
	}
	r.update(func(rel *store.Release) { rel.ImageSummary = sum })
	r.rel.ImageSummary = sum
	fmt.Fprintf(w, "镜像内容摘要: %s\n", sum)

	name := "btd-build"
	if src := r.refsForImage(r.rel.Image); len(src) == 0 {
		return nil
	}
	fp26 := buildstage.Short(r.fingerprintKey(r.rel.ImageFP), 26)
	dir := "/tmp/btd-probe"
	script := fmt.Sprintf(
		"mkdir -p %[1]s && printf 'FROM %%s\n' %[2]s > %[1]s/Dockerfile && printf 'LABEL build-to-deploy.fingerprint=%[3]s\n' >> %[1]s/Dockerfile && docker build -t %[4]s -f %[1]s/Dockerfile %[1]s",
		dir, shellQuote(sum), fp26, shellQuote(name))
	fmt.Fprintf(w, "记录构建输入指纹标签 %s=%s，供下次发布直接复用镜像\n", name, fp26)
	if err := r.engine.SSH.RunCommand(r.ctx, r.srv, script, w); err != nil {
		return fmt.Errorf("写入指纹标签失败: %w", err)
	}
	return nil
}

// checkBuiltImage 构建完成后比对镜像内容与已知版本：命中即说明"换了 Tag 的同一个镜像"，只提示不阻断
func (r *pipelineRunner) checkBuiltImage(w io.Writer) error {
	if r.rel.ImageFP == "" {
		return nil
	}
	sum := r.inspectImage(r.rel.Image)
	if sum == "" {
		return nil
	}
	r.update(func(rel *store.Release) { rel.ImageSummary = sum })
	r.rel.ImageSummary = sum
	fmt.Fprintf(w, "镜像内容摘要: %s\n", sum)

	// 内容摘要无法从构建输入推算（基础镜像更新、RUN 指令等），这里以实际构建结果为准再比对一次
	hit := 0
	for _, rel := range r.engine.Store.List(r.app.ID, 0) {
		if rel.ID == r.rel.ID {
			continue
		}
		if rel.ImageSummary == sum && rel.Tag != r.rel.Tag && hit < 3 {
			hit++
			fmt.Fprintf(w, "[警告] 新构建的镜像与历史版本 %s 内容完全相同（摘要 %s）：换 Tag 并不会产生新的镜像内容\n", rel.Tag, sum)
		}
	}
	if cur := r.precheck.CurrentImage; cur != "" && cur == sum && cur == r.rel.PrevImage && r.precheck.CurrentTag == r.rel.Tag {
		// 构建命令不依赖产物（如只做 docker build、Tag 与产物无关）时，换 Tag 会被误判为"新版本"：
		// 线上镜像一个字都不会变，这种情况直接拒绝，避免留下一条实际没生效的发布记录
		return fmt.Errorf("新构建的镜像与线上正在运行的镜像完全相同（摘要 %s），本次发布不会改变线上服务；"+
			"请确认构建命令确实使用了新产物（或修改 Tag 之外的内容），如构建命令不引用产物，请调整后重试", sum)
	}
	if cur := r.precheck.CurrentImage; cur != "" && cur == sum {
		fmt.Fprintln(w, "[警告] 新构建的镜像与线上当前运行的镜像内容完全相同，本次发布不会改变线上镜像")
	}
	return nil
}

// deployDocker 让宿主机上以指定镜像运行（同名容器先移除再启动）。
// 镜像引用只允许合法镜像字符，命令模板里的变量同样通过 shellQuote 转义。
func (r *pipelineRunner) deployDocker(w io.Writer, imageRef string) error {
	if !imageRefRe.MatchString(imageRef) {
		return fmt.Errorf("镜像引用不合法: %q", imageRef)
	}
	if err := dockerrun.Validate(r.app); err != nil {
		return err
	}
	cmd, err := dockerrun.RunCmd(r.app, imageRef)
	if err != nil {
		return err
	}
	if custom := strings.TrimSpace(r.app.DeployCmd); custom != "" && !strings.HasPrefix(custom, "docker run -d --name {{.Container}} ") {
		// 自定义部署命令：保留用户完全控制的能力，变量与内置命令同样经过校验与转义
		rendered, err := r.renderCmd(custom)
		if err != nil {
			return fmt.Errorf("渲染部署命令失败: %w", err)
		}
		fmt.Fprintln(w, "使用自定义容器部署命令")
		cmd = rendered
	}
	fmt.Fprintf(w, "正在宿主机上启动容器 %s ...\n", r.app.DockerContainer)
	return r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w)
}

// awaitDocker 轮询容器状态（以及可选的容器内 HTTP 探测），直到 running / 失败 / 超时
func (r *pipelineRunner) awaitDocker(w io.Writer) error {
	cmd, err := dockerrun.ReadyCmd(r.app, r.app.RolloutTimeout)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "等待容器 %s 就绪 ...\n", r.app.DockerContainer)
	if err := r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w); err != nil {
		return fmt.Errorf("容器 %s 未在 %d 秒内就绪: %w", r.app.DockerContainer, r.app.RolloutTimeout, err)
	}
	fmt.Fprintf(w, "✔ 容器 %s 已就绪\n", r.app.DockerContainer)
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
	if r.app.DeployMode == config.DeployModeDocker {
		r.autoRollbackDocker(cause, reason)
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
		if err := r.engine.SSH.RunCommand(ctx, r.srv, rbCmd, r.hub); err != nil {
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

// autoRollbackDocker 容器形态的自动回滚：把上一版本镜像重新起成容器
func (r *pipelineRunner) autoRollbackDocker(cause error, reason string) {
	if r.ctx.Err() != nil || errors.Is(cause, context.Canceled) {
		r.hub.Logf("\n[警告] 发布已被取消，跳过自动回滚（容器可能仍停留在新版本，请确认）")
		return
	}
	r.hub.Logf(fmt.Sprintf("\n[警告] %s，触发自动回滚到上一版本: %s", reason, r.rel.PrevImage))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	go func() {
		select {
		case <-r.ctx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	savedCtx := r.ctx
	r.ctx = ctx
	err := r.deployDocker(r.hub, r.rel.PrevImage)
	if err == nil {
		err = r.awaitDocker(r.hub)
	}
	r.ctx = savedCtx
	if err == nil {
		r.update(func(rel *store.Release) {
			rel.Status = store.StatusRolledBack
		})
		r.hub.Logf("✔ 已自动回滚至上一版本容器")
	} else {
		r.hub.Logf("[错误] 自动回滚未能完成，请手动确认容器版本: " + err.Error())
	}
}

func (r *pipelineRunner) runRollback() error {
	r.hub.Logf(fmt.Sprintf("==> 🚨 开始回滚应用: %s [目标版本: %s]", r.app.Name, r.rel.Tag))
	r.hub.Logf(fmt.Sprintf("==> 目标镜像: %s", r.rel.Image))
	r.hub.Logf(fmt.Sprintf("==> 已确认目标镜像仍存在于服务器 %s 上", r.srv.Host))
	if r.rel.RollbackOf != "" {
		r.hub.Logf(fmt.Sprintf("==> 关联历史版本记录: %s", r.rel.RollbackOf))
	}

	if r.app.DeployMode == config.DeployModeDocker {
		if err := r.stage("deploy", func(w io.Writer) error {
			return r.deployDocker(w, r.rel.Image)
		}); err != nil {
			return err
		}
		return r.stage("rollout", func(w io.Writer) error {
			return r.awaitDocker(w)
		})
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
	// 先释放"该应用正在发布"的占位，再写入终态：否则调用方看到 done 后立刻发起回滚/重发，
	// 会撞上尚未清理的 running 记录（表现为"应用正在发布中"，实际流水线已结束）
	r.engine.releaseRunning(r.app.ID)
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

// releaseRunning 释放应用级发布占位（幂等，可由 finish 与收尾 goroutine 重复调用）
func (e *Engine) releaseRunning(appID string) {
	e.mu.Lock()
	delete(e.running, appID)
	e.mu.Unlock()
}

func (r *pipelineRunner) cleanup() {
	r.hub.Logf("\n==> 阶段 [cleanup]: 自动清理历史版本产物")
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
			_ = r.engine.SSH.RunCommand(context.Background(), r.srv, cleanCmd, io.Discard)
			r.hub.Logf(fmt.Sprintf("✔ 已清理远程产物目录 %s，保留最近 %d 个版本包", r.app.ArtifactDir, keepCount))
		}
	}

	// 清理本次上传产生的本地 .part 副本（上传目录只是缓存）
	uploadsDir := filepath.Join(r.engine.DataDir, "uploads") + string(os.PathSeparator)
	if strings.HasPrefix(r.artifact, uploadsDir) {
		_ = os.Remove(r.artifact)
	}

	// 注意：不自动删除远程 Docker 镜像（包括悬空层与旧版本 tag），历史版本由运维自行管理，
	// 避免误删回滚依赖的镜像。
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
