package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"build-to-deploy/internal/config"
	"build-to-deploy/internal/store"
	"build-to-deploy/internal/upload"
)

// YamlBackupMeta 远程 YAML 快照的元信息，和快照内容一起保存在 yamlBackupDir/<appID>/ 下。
// 内容文件与元信息文件同名，扩展名分别为 .yaml / .json。
type YamlBackupMeta struct {
	Name          string    `json:"name"` // 快照文件名（含 .yaml），下载/恢复时使用
	ReleaseID     string    `json:"releaseId"`
	AppID         string    `json:"appId"`
	AppName       string    `json:"appName"`
	ServerID      string    `json:"serverId,omitempty"`
	ServerName    string    `json:"serverName,omitempty"`
	ServerHost    string    `json:"serverHost,omitempty"`
	YamlPath      string    `json:"yamlPath"`
	TargetImage   string    `json:"targetImage,omitempty"`
	PreviousImage string    `json:"previousImage,omitempty"`
	Tag           string    `json:"tag,omitempty"`
	OperatorIP    string    `json:"operatorIp,omitempty"`
	Kind          string    `json:"kind,omitempty"` // deploy | rollback | restore | pre-restore
	CreatedAt     time.Time `json:"createdAt"`
	Size          int64     `json:"size"`
	SHA256        string    `json:"sha256"`
}

func (r *pipelineRunner) yamlBackupDir() string {
	return filepath.Join(r.engine.Cfg.ServerSettingsSnapshot().YamlBackupDir, r.app.ID)
}

// yamlBackupName 快照基础名：发布记录 ID + 目标镜像，保证同一次发布可追溯且不覆盖历史
func yamlBackupName(releaseID, targetImage, suffix string) string {
	base := releaseID
	if strings.TrimSpace(suffix) != "" {
		base += "_" + sanitizeYamlBackupPart(suffix)
	}
	name := base + "_" + sanitizeYamlBackupPart(targetImage)
	// 文件名上限留足余量（目录/扩展名另算），超长镜像名只截断展示部分，不影响唯一性
	if len(name) > 180 {
		name = name[:180]
	}
	return name
}

// firstYamlImage 返回 YAML 中第一个非注释 image: 的值，用于记录"改之前是什么镜像"
func firstYamlImage(content string) string {
	for _, line := range strings.Split(content, "\n") {
		_, value, _, ok := splitImagePrefix(line)
		if ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// saveYamlSnapshot 在修改远程文件之前落一份本地快照。写内容或写元信息任何一步失败都返回错误，
// 由调用方中止发布——备份失败却继续改线上文件，这个备份就没有意义。
func (r *pipelineRunner) saveYamlSnapshot(w io.Writer, rawYaml, yamlPath, targetImage, nameSuffix string) (string, error) {
	dir := r.yamlBackupDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("创建远程 YAML 备份目录失败: %w", err)
	}

	base := yamlBackupName(r.rel.ID, targetImage, nameSuffix)
	backupYamlPath := filepath.Join(dir, base+".yaml")
	metaPath := filepath.Join(dir, base+".json")

	if err := os.WriteFile(backupYamlPath, []byte(rawYaml), 0o600); err != nil {
		return "", fmt.Errorf("保存远程 YAML 备份失败: %w", err)
	}

	sum := sha256.Sum256([]byte(rawYaml))
	meta := YamlBackupMeta{
		Name:          base + ".yaml",
		ReleaseID:     r.rel.ID,
		AppID:         r.app.ID,
		AppName:       r.app.Name,
		ServerID:      r.srv.ID,
		ServerName:    r.srv.Name,
		ServerHost:    r.srv.Host,
		YamlPath:      yamlPath,
		TargetImage:   targetImage,
		PreviousImage: firstYamlImage(rawYaml),
		Tag:           r.rel.Tag,
		OperatorIP:    r.rel.OperatorIP,
		Kind:          r.rel.Kind,
		CreatedAt:     time.Now(),
		Size:          int64(len(rawYaml)),
		SHA256:        hex.EncodeToString(sum[:]),
	}
	b, err := json.MarshalIndent(meta, "", "  ")
	if err == nil {
		tmp := metaPath + ".tmp"
		if err = os.WriteFile(tmp, b, 0o600); err == nil {
			err = os.Rename(tmp, metaPath)
		}
	}
	if err != nil {
		// 元信息写失败时删掉半份快照，避免列表里出现无法识别来源的文件
		_ = os.Remove(backupYamlPath)
		_ = os.Remove(metaPath + ".tmp")
		return "", fmt.Errorf("保存远程 YAML 备份元信息失败: %w", err)
	}

	r.pruneYamlBackups(w, base+".yaml")
	fmt.Fprintf(w, "✔ 已保存修改前远程 YAML 备份: %s\n", backupYamlPath)
	return backupYamlPath, nil
}

// pruneYamlBackups 按份数与天数清理旧快照；每次写完新快照后调用，protect 为本次文件，永不删除。
func (r *pipelineRunner) pruneYamlBackups(w io.Writer, protect string) {
	cfg := r.engine.Cfg.ServerSettingsSnapshot()
	keep := cfg.YamlBackupKeep
	maxAgeDays := cfg.YamlBackupMaxAgeDays
	if keep < 0 && maxAgeDays < 0 {
		return
	}
	removed, err := pruneYamlBackupDir(r.yamlBackupDir(), keep, maxAgeDays, protect)
	if err != nil {
		fmt.Fprintf(w, "[警告] 清理旧 YAML 备份失败: %v\n", err)
		return
	}
	if removed > 0 {
		fmt.Fprintf(w, "✔ 已按保留策略清理 %d 份旧 YAML 备份\n", removed)
	}
}

func pruneYamlBackupDir(dir string, keep, maxAgeDays int, protect string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	type item struct {
		name    string
		created time.Time
	}
	var items []item
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".yaml") {
			continue
		}
		created := time.Time{}
		if b, err := os.ReadFile(filepath.Join(dir, strings.TrimSuffix(e.Name(), ".yaml")+".json")); err == nil {
			var meta YamlBackupMeta
			if json.Unmarshal(b, &meta) == nil {
				created = meta.CreatedAt
			}
		}
		if created.IsZero() {
			if info, err := e.Info(); err == nil {
				created = info.ModTime()
			}
		}
		items = append(items, item{name: e.Name(), created: created})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].created.After(items[j].created) })

	cutoff := time.Time{}
	if maxAgeDays > 0 {
		cutoff = time.Now().Add(-time.Duration(maxAgeDays) * 24 * time.Hour)
	}
	removed := 0
	for i, it := range items {
		if it.name == protect {
			continue
		}
		tooMany := keep > 0 && i >= keep
		tooOld := !cutoff.IsZero() && it.created.Before(cutoff)
		if !tooMany && !tooOld {
			continue
		}
		base := strings.TrimSuffix(it.name, ".yaml")
		if err := os.Remove(filepath.Join(dir, it.name)); err != nil && !os.IsNotExist(err) {
			return removed, err
		}
		_ = os.Remove(filepath.Join(dir, base+".json"))
		removed++
	}
	return removed, nil
}

// ListYamlBackups 列出全部应用的远程 YAML 快照（只读本地目录，不访问服务器）
func ListYamlBackups(cfg *config.Config) ([]YamlBackupMeta, error) {
	root := cfg.ServerSettingsSnapshot().YamlBackupDir
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return []YamlBackupMeta{}, nil
		}
		return nil, err
	}
	out := make([]YamlBackupMeta, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		list, err := listYamlBackupsForApp(cfg, e.Name())
		if err != nil {
			continue
		}
		out = append(out, list...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func listYamlBackupsForApp(cfg *config.Config, appID string) ([]YamlBackupMeta, error) {
	dir := filepath.Join(cfg.ServerSettingsSnapshot().YamlBackupDir, appID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	app := cfg.FindApp(appID)
	// 应用可能已被删除，仍保留其历史快照供查看
	serverID, serverName, serverHost := "", "", ""
	if app != nil {
		serverID = app.ServerID
		if srv := cfg.FindServer(app.ServerID); srv != nil {
			serverName, serverHost = srv.Name, srv.Host
		}
	}
	out := make([]YamlBackupMeta, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".yaml") {
			continue
		}
		metaPath := filepath.Join(dir, strings.TrimSuffix(e.Name(), ".yaml")+".json")
		var meta YamlBackupMeta
		if b, err := os.ReadFile(metaPath); err == nil {
			_ = json.Unmarshal(b, &meta)
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if meta.Name == "" {
			meta.Name = e.Name()
		}
		if meta.AppID == "" {
			meta.AppID = appID
		}
		if meta.AppName == "" && app != nil {
			meta.AppName = app.Name
		}
		if meta.ServerID == "" {
			meta.ServerID = serverID
		}
		if meta.ServerName == "" {
			meta.ServerName = serverName
		}
		if meta.ServerHost == "" {
			meta.ServerHost = serverHost
		}
		if meta.CreatedAt.IsZero() {
			meta.CreatedAt = info.ModTime()
		}
		if meta.Size == 0 {
			meta.Size = info.Size()
		}
		out = append(out, meta)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

// ReadYamlBackup 按应用与文件名读取一份快照；只接受纯文件名，避免路径穿越。
func ReadYamlBackup(cfg *config.Config, appID, name string) (YamlBackupMeta, string, error) {
	var meta YamlBackupMeta
	if !config.IDRe.MatchString(appID) {
		return meta, "", errors.New("应用 ID 不合法")
	}
	if name == "" || filepath.Base(name) != name || !strings.HasSuffix(strings.ToLower(name), ".yaml") {
		return meta, "", errors.New("备份文件名不合法")
	}
	dir := filepath.Join(cfg.ServerSettingsSnapshot().YamlBackupDir, appID)
	content, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return meta, "", fmt.Errorf("读取备份失败: %w", err)
	}
	metaPath := filepath.Join(dir, strings.TrimSuffix(name, ".yaml")+".json")
	if b, err := os.ReadFile(metaPath); err == nil {
		_ = json.Unmarshal(b, &meta)
	}
	if meta.AppID != "" && meta.AppID != appID {
		return meta, "", errors.New("备份归属应用不一致")
	}
	if meta.Name == "" {
		meta.Name = name
	}
	return meta, string(content), nil
}

// YamlRestoreRequest 恢复一份远程 YAML 快照。Apply=true 时会写回文件后执行部署命令并等待就绪。
type YamlRestoreRequest struct {
	AppID    string `json:"appId"`
	Name     string `json:"name"`
	Apply    bool   `json:"apply"`
	ClientIP string `json:"-"`
	Remark   string `json:"remark"`
}

// StartYamlRestore 创建一条 restore 发布记录并异步执行，复用现有日志/状态推送。
func (e *Engine) StartYamlRestore(req YamlRestoreRequest) (*store.Release, error) {
	app := e.Cfg.FindApp(req.AppID)
	if app == nil {
		return nil, fmt.Errorf("应用 %s 不存在", req.AppID)
	}
	meta, content, err := ReadYamlBackup(e.Cfg, app.ID, req.Name)
	if err != nil {
		return nil, err
	}
	if meta.ServerID != "" && meta.ServerID != app.ServerID {
		return nil, fmt.Errorf("该备份来自服务器 %s，当前应用绑定的是 %s，已拒绝恢复", meta.ServerID, app.ServerID)
	}
	srv := e.Cfg.FindServer(app.ServerID)
	if srv == nil {
		return nil, fmt.Errorf("未找到服务器配置: %s", app.ServerID)
	}
	yamlPath := strings.TrimSpace(meta.YamlPath)
	if yamlPath == "" {
		yamlPath = k8sYamlFilePath(app)
	}
	if !isYamlPath(yamlPath) {
		return nil, errors.New("该备份没有记录具体 YAML 文件路径，无法恢复")
	}
	if cur := k8sYamlFilePath(app); isYamlPath(cur) && cur != yamlPath {
		return nil, fmt.Errorf("备份对应的 YAML 路径 (%s) 与当前应用配置 (%s) 不一致，请先确认应用配置", yamlPath, cur)
	}
	if req.Apply {
		if err := validateAppForPipeline(app); err != nil {
			return nil, err
		}
		// 恢复文件只对 YAML 文件模式有意义；set image 等自管镜像的命令不会读取刚写回的文件
		if !isYamlPath(k8sYamlFilePath(app)) {
			return nil, errors.New("当前应用不是 K8s YAML 文件模式，不能执行“恢复并发布”；可只恢复文件")
		}
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
			e.releaseRun(app.ID, run.releaseID)
		}
	}()

	stages := []store.Stage{{Name: "deploy", Title: "恢复远程 YAML 文件", Status: store.StatusPending}}
	if req.Apply {
		stages[0].Title = "恢复 YAML 并执行部署"
		stages = append(stages, store.Stage{Name: "rollout", Title: "等待 Pod 滚动就绪", Status: store.StatusPending})
	}
	image := meta.TargetImage
	if image == "" {
		image = firstYamlImage(content)
	}
	remark := strings.TrimSpace(req.Remark)
	if remark == "" {
		remark = fmt.Sprintf("恢复 YAML 备份 [%s]", req.Name)
	}
	rel := &store.Release{
		ID:         relID,
		App:        app.ID,
		Kind:       "restore",
		Tag:        meta.Tag,
		Image:      image,
		Operator:   srv.User + "@" + srv.Host,
		OperatorIP: req.ClientIP,
		Remark:     remark,
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
		defer e.releaseRun(app.ID, run.releaseID)

		runner := &pipelineRunner{
			engine: e,
			ctx:    ctx,
			rel:    rel,
			app:    app,
			srv:    srv,
			hub:    hub,
		}
		var previous string
		err := runner.stage("deploy", func(w io.Writer) error {
			var err error
			previous, err = runner.restoreYaml(w, yamlPath, content, req.Apply)
			return err
		})
		if err == nil && req.Apply {
			err = runner.stage("rollout", func(w io.Writer) error {
				return e.Kube.RolloutStatus(ctx, w, srv, app.Namespace, app.Deployment, app.RolloutTimeout)
			})
			if err != nil && previous != "" {
				runner.revertRestoredYaml(previous, yamlPath)
			}
		}
		runner.finish(err)
	}()

	return rel, nil
}

// restoreYaml 写回快照前，先把当前远程文件再存一份，确保恢复本身也能反悔。
func (r *pipelineRunner) restoreYaml(w io.Writer, yamlPath, content string, apply bool) (string, error) {
	fmt.Fprintf(w, "📄 正在恢复远程 YAML 配置文件: %s ...\n", yamlPath)
	current, err := r.engine.SSH.ReadRemoteFile(r.srv, yamlPath)
	if err != nil {
		return "", fmt.Errorf("读取当前远程 YAML 文件 (%s) 失败: %w", yamlPath, err)
	}
	if _, err := r.saveYamlSnapshot(w, current, yamlPath, firstYamlImage(current), "pre-restore"); err != nil {
		return "", err
	}
	if err := r.engine.SSH.WriteRemoteFile(r.srv, yamlPath, content); err != nil {
		return "", fmt.Errorf("写回远程 YAML 文件 (%s) 失败: %w", yamlPath, err)
	}
	fmt.Fprintln(w, "✔ 已写回备份中的 YAML 内容")
	if !apply {
		return current, nil
	}
	cmd, err := r.renderCmd(r.app.DeployCmd)
	if err != nil {
		return current, fmt.Errorf("渲染部署命令失败: %w", err)
	}
	if err := r.engine.SSH.RunCommand(r.ctx, r.srv, cmd, w); err != nil {
		r.revertRestoredYaml(current, yamlPath)
		return current, fmt.Errorf("部署命令执行失败: %w", err)
	}
	return current, nil
}

// revertRestoredYaml 把恢复前的文件写回并 apply，避免 YAML 与线上停留在半套状态。
func (r *pipelineRunner) revertRestoredYaml(previous, yamlPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := r.engine.SSH.WriteRemoteFile(r.srv, yamlPath, previous); err != nil {
		r.hub.Logf("[错误] 恢复前的 YAML 文件写回失败: " + err.Error())
		return
	}
	ns := r.app.Namespace
	if ns == "" {
		ns = "default"
	}
	cmd := fmt.Sprintf("kubectl -n %s apply -f %s", shellQuote(ns), shellQuote(yamlPath))
	if err := r.engine.SSH.RunCommand(ctx, r.srv, cmd, r.hub); err != nil {
		r.hub.Logf("[错误] 恢复前的 YAML apply 失败: " + err.Error())
		return
	}
	r.hub.Logf("✔ 已退回恢复前的 YAML 并重新 apply")
}
