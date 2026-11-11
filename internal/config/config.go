package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"build-to-deploy/internal/sshpool"
	"gopkg.in/yaml.v3"
)

type Config struct {
	mu         sync.RWMutex            `yaml:"-" json:"-"`
	path       string                  `yaml:"-" json:"-"`
	Server     ServerSettings          `yaml:"server" json:"server"`
	SSHServers []*sshpool.ServerConfig `yaml:"sshServers" json:"sshServers"`
	Apps       []*AppConfig            `yaml:"apps" json:"apps"`
}

type ServerSettings struct {
	Listen      string `yaml:"listen" json:"listen"`           // 默认 ":8080"
	DataDir     string `yaml:"dataDir" json:"dataDir"`         // 本地缓存、日志存放目录
	MaxUploadMB int64  `yaml:"maxUploadMB" json:"maxUploadMB"` // 单个上传最大 MB
	DryRun      bool   `yaml:"dryRun" json:"dryRun"`           // 是否演练模式（无需连真机）
}

type AppConfig struct {
	ID          string `yaml:"id" json:"id"`
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description" json:"description"`
	ServerID    string `yaml:"serverId" json:"serverId"` // 关联的 SSH 服务器 ID
	Type        string `yaml:"type" json:"type"`         // jar | go | binary

	// 本地编译配置（可选，支持本地一键编译后再通过 VPN 上传产物，也可不编译直接上传）
	LocalProjectDir   string `yaml:"localProjectDir" json:"localProjectDir"`     // 本地项目源码目录，如 /Users/mac/work/order-service
	LocalBuildCmd     string `yaml:"localBuildCmd" json:"localBuildCmd"`         // 本地编译命令，如 mvn clean package -DskipTests
	LocalArtifactPath string `yaml:"localArtifactPath" json:"localArtifactPath"` // 本地编译生成的默认产物路径（可相对 LocalProjectDir），如 target/order-service.jar

	// 目录分离配置（满足需求 2）
	ArtifactDir string `yaml:"artifactDir" json:"artifactDir"` // 产物单独目录，如 /data/artifacts/order-service
	BuildDir    string `yaml:"buildDir" json:"buildDir"`       // 镜像构建单独目录，如 /data/docker-build/order-service
	K8sYamlDir  string `yaml:"k8sYamlDir" json:"k8sYamlDir"`   // K8s YAML 配置目录，如 /data/k8s-yaml
	K8sYamlFile string `yaml:"k8sYamlFile" json:"k8sYamlFile"` // K8s YAML 配置文件名，如 order-service.yaml (支持多个服务共用目录不同文件名)

	// 镜像与部署命令（根据需求 2 灵活配置命令）
	Image          string `yaml:"image" json:"image"`                   // 镜像名称（不含tag）
	BuildCmd       string `yaml:"buildCmd" json:"buildCmd"`             // 构建命令模板
	PushCmd        string `yaml:"pushCmd" json:"pushCmd"`               // 推送命令模板（可选）
	DeployCmd      string `yaml:"deployCmd" json:"deployCmd"`           // 部署命令模板
	Namespace      string `yaml:"namespace" json:"namespace"`           // 默认命名空间
	Deployment     string `yaml:"deployment" json:"deployment"`         // Deployment 名字
	Container      string `yaml:"container" json:"container"`           // 容器名称
	RolloutTimeout int    `yaml:"rolloutTimeout" json:"rolloutTimeout"` // 等待就绪超时（秒）
	AutoRollback   bool   `yaml:"autoRollback" json:"autoRollback"`     // 失败是否自动回滚
}

var (
	TagRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$`)
	// IDRe 服务器/应用 ID：会出现在日志文件名与前端模板中，限制为安全字符
	IDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	// ImageRe 镜像名（不含 Tag）：拒绝空格、分号、$、引号等可进入 shell 的字符
	ImageRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/:@-]*$`)
	// RemotePathRe 远程绝对路径：只允许常见安全字符
	RemotePathRe = regexp.MustCompile(`^/[A-Za-z0-9._/-]*$`)
	// K8sYamlFileRe K8s YAML 文件名：不得包含目录分隔符或 ..
	K8sYamlFileRe = regexp.MustCompile(`^[A-Za-z0-9._-]+\.ya?ml$`)
	// k8sNameRe K8s 资源名（RFC 1123 label）
	k8sNameRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// ValidateApp 校验会拼入远程 shell 命令或决定远程写入位置的字段，防止命令注入与任意文件写
func ValidateApp(a *AppConfig) error {
	if !IDRe.MatchString(a.ID) {
		return fmt.Errorf("应用 ID 只能包含字母、数字、. _ -，且以字母或数字开头 (当前: %q)", a.ID)
	}
	if a.Namespace != "" && !k8sNameRe.MatchString(a.Namespace) {
		return fmt.Errorf("命名空间不合法 (当前: %q)", a.Namespace)
	}
	if a.Deployment != "" && !k8sNameRe.MatchString(a.Deployment) {
		return fmt.Errorf("Deployment 名称不合法 (当前: %q)", a.Deployment)
	}
	if a.Container != "" && !k8sNameRe.MatchString(a.Container) {
		return fmt.Errorf("容器名称不合法 (当前: %q)", a.Container)
	}
	if strings.TrimSpace(a.Image) != "" && !ImageRe.MatchString(a.Image) {
		return fmt.Errorf("镜像名不合法 (当前: %q)", a.Image)
	}
	for label, p := range map[string]string{"产物目录": a.ArtifactDir, "构建目录": a.BuildDir, "K8s 目录": a.K8sYamlDir} {
		if strings.TrimSpace(p) == "" {
			continue
		}
		// 根目录与 .. 会导致清理或构建命令越界到系统目录，必须拒绝
		if p == "/" {
			return fmt.Errorf("%s不能为根目录 / (当前: %q)", label, p)
		}
		if strings.Contains(p, "..") {
			return fmt.Errorf("%s不能包含 .. (当前: %q)", label, p)
		}
		if !RemotePathRe.MatchString(p) {
			return fmt.Errorf("%s必须是绝对路径且不能包含空格、; $ & | 引号等字符 (当前: %q)", label, p)
		}
	}
	if strings.TrimSpace(a.K8sYamlFile) != "" && !K8sYamlFileRe.MatchString(a.K8sYamlFile) {
		return fmt.Errorf("K8s YAML 文件名不合法，只能是如 order-service.yaml (当前: %q)", a.K8sYamlFile)
	}
	return nil
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			cfg := Default()
			cfg.path = path
			_ = cfg.Save()
			return cfg, nil
		}
		return nil, err
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(b, cfg); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}
	cfg.path = path
	cfg.normalize()
	return cfg, nil
}

func Default() *Config {
	return &Config{
		Server: ServerSettings{
			Listen:      "127.0.0.1:8080",
			DataDir:     "./data",
			MaxUploadMB: 2048,
			DryRun:      false,
		},
		SSHServers: []*sshpool.ServerConfig{
			{
				ID:       "default-linux",
				Name:     "K8s 构建/部署服务器",
				Host:     "192.168.1.100",
				Port:     22,
				User:     "root",
				AuthType: "password",
				Password: "",
			},
		},
		Apps: []*AppConfig{
			{
				ID:             "order-service",
				Name:           "order-service",
				Description:    "订单服务",
				ServerID:       "default-linux",
				Type:           "jar",
				ArtifactDir:    "/data/packages/order-service",
				BuildDir:       "/data/docker-build/order-service",
				K8sYamlDir:     "/data/k8s-yaml",
				K8sYamlFile:    "order-service.yaml",
				Image:          "harbor.example.com/app/order-service",
				BuildCmd:       "cd {{.BuildDir}} && docker build -t {{.Image}}:{{.Tag}} .",
				PushCmd:        "",
				DeployCmd:      "kubectl apply -f {{.K8sYamlPath}}",
				Namespace:      "prod",
				Deployment:     "order-service",
				Container:      "order-service",
				RolloutTimeout: 300,
				AutoRollback:   true,
			},
		},
	}
}

func (c *Config) normalize() {
	if c.Server.Listen == "" {
		// 默认只监听本机回环地址：服务无认证，不应暴露到局域网
		c.Server.Listen = "127.0.0.1:8080"
	}
	if c.Server.DataDir == "" {
		c.Server.DataDir = "./data"
	}
	if c.Server.MaxUploadMB <= 0 {
		c.Server.MaxUploadMB = 2048
	}
	// 上限 1 PB：再大就会在 <<20 换算字节时溢出为负数，使上传大小校验失效
	if c.Server.MaxUploadMB > 1<<30 {
		c.Server.MaxUploadMB = 1 << 30
	}

	for _, app := range c.Apps {
		if app.ID == "" {
			app.ID = app.Name
		}
		if app.BuildCmd == "" {
			app.BuildCmd = "cd {{.BuildDir}} && docker build -t {{.Image}}:{{.Tag}} ."
		}
		if app.DeployCmd == "" {
			if app.K8sYamlFile != "" {
				app.DeployCmd = "kubectl apply -f {{.K8sYamlPath}}"
			} else if app.K8sYamlDir != "" {
				app.DeployCmd = "kubectl apply -f {{.K8sYamlDir}}"
			} else {
				app.DeployCmd = "kubectl -n {{.Namespace}} set image deployment/{{.Deployment}} {{.Container}}={{.Image}}:{{.Tag}}"
			}
		}
		if app.Namespace == "" {
			app.Namespace = "default"
		}
		if app.Deployment == "" {
			app.Deployment = app.Name
		}
		if app.Container == "" {
			app.Container = app.Deployment
		}
		if app.RolloutTimeout <= 0 {
			app.RolloutTimeout = 300
		}
		// 规范化路径统一为 linux 斜杠（保留根路径 "/"，避免 "cp -f x /" 之类命令被改写）
		app.ArtifactDir = trimRemoteSlash(app.ArtifactDir)
		app.BuildDir = trimRemoteSlash(app.BuildDir)
		app.K8sYamlDir = trimRemoteSlash(app.K8sYamlDir)
		// YAML 文件名只允许纯文件名（拒绝 ../ 目录穿越）
		if app.K8sYamlFile != "" && strings.ContainsAny(app.K8sYamlFile, "/\\") {
			app.K8sYamlFile = filepath.Base(app.K8sYamlFile)
		}
	}
}

func trimRemoteSlash(p string) string {
	p = filepath.ToSlash(p)
	if p == "/" {
		return p
	}
	return strings.TrimRight(p, "/")
}

func (c *Config) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saveUnlocked()
}

// FindApp 返回应用的副本：配置可在网页上随时被替换，直接返回内部指针会让调用方读到半更新数据
func (c *Config) FindApp(idOrName string) *AppConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, a := range c.Apps {
		if a.ID == idOrName || a.Name == idOrName {
			cp := *a
			return &cp
		}
	}
	return nil
}

// FindServer 按 ID 精确查找服务器配置，找不到返回 nil。
// 禁止兜底返回第一台：调用方会据此操作远程主机，兜底会静默连错机器
func (c *Config) FindServer(id string) *sshpool.ServerConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.SSHServers {
		if s.ID == id {
			cp := *s
			return &cp
		}
	}
	return nil
}

// ListServers 返回服务器配置的快照，供 API 层序列化
func (c *Config) ListServers() []*sshpool.ServerConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*sshpool.ServerConfig, 0, len(c.SSHServers))
	for _, s := range c.SSHServers {
		cp := *s
		out = append(out, &cp)
	}
	return out
}

// ListApps 返回应用配置的快照，供 API 层序列化
func (c *Config) ListApps() []*AppConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*AppConfig, 0, len(c.Apps))
	for _, a := range c.Apps {
		cp := *a
		out = append(out, &cp)
	}
	return out
}

// ServerSettingsSnapshot 返回 server 段配置的只读快照
func (c *Config) ServerSettingsSnapshot() ServerSettings {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Server
}

func (c *Config) UpdateServer(s *sshpool.ServerConfig) error {
	if s == nil || !IDRe.MatchString(s.ID) {
		return fmt.Errorf("服务器 ID 不合法，只能包含字母、数字、. _ -")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	found := false
	for i, old := range c.SSHServers {
		if old.ID == s.ID {
			cp := *s
			c.SSHServers[i] = &cp
			found = true
			break
		}
	}
	if !found {
		cp := *s
		c.SSHServers = append(c.SSHServers, &cp)
	}
	return c.saveUnlocked()
}

func (c *Config) DeleteServer(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var updated []*sshpool.ServerConfig
	for _, s := range c.SSHServers {
		if s.ID != id {
			updated = append(updated, s)
		}
	}
	c.SSHServers = updated
	return c.saveUnlocked()
}

func (c *Config) UpdateApp(app *AppConfig) error {
	if app == nil {
		return fmt.Errorf("应用配置不能为空")
	}
	if err := ValidateApp(app); err != nil {
		return err
	}
	if !IDRe.MatchString(app.Name) {
		return fmt.Errorf("应用名称会同时用作应用 ID，只能包含字母、数字、. _ -，且以字母或数字开头 (当前: %q)", app.Name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	found := false
	for i, old := range c.Apps {
		if old.ID == app.ID {
			cp := *app
			c.Apps[i] = &cp
			found = true
			break
		}
	}
	if !found {
		cp := *app
		c.Apps = append(c.Apps, &cp)
	}
	return c.saveUnlocked()
}

// UpdateLocalArtifactPath 只更新某个应用的本地产物路径（发布时用户临时选择新文件）。
// 加锁顺序必须与 Save 一致：先 c.mu 再走导出，否则与保存并发时会死锁。
func (c *Config) UpdateLocalArtifactPath(appID, path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, a := range c.Apps {
		if a.ID == appID || a.Name == appID {
			cp := *a
			cp.LocalArtifactPath = path
			if err := ValidateApp(&cp); err != nil {
				return err
			}
			c.Apps[i] = &cp
			return c.saveUnlocked()
		}
	}
	return fmt.Errorf("应用 %s 不存在", appID)
}

func (c *Config) DeleteApp(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var updated []*AppConfig
	for _, a := range c.Apps {
		if a.ID != id {
			updated = append(updated, a)
		}
	}
	c.Apps = updated
	return c.saveUnlocked()
}

func (c *Config) saveUnlocked() error {
	b, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	if c.path == "" {
		c.path = "configs/config.yaml"
	}
	// 写入前备份上一版：配置页每次保存都留档，改错可一键恢复。
	// 备份失败（磁盘满、目录只读）不能让保存本身失败，否则用户无法修改配置
	if _, statErr := os.Stat(c.path); statErr == nil {
		_ = c.backupLocked()
	}
	// 原子写入：避免进程中断留下截断的配置文件；权限收紧到 0600（含 SSH 明文密码）
	if dir := filepath.Dir(c.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}

// maxConfigBackups 配置备份保留份数
const maxConfigBackups = 20

// backupNameRe 备份文件名格式：恢复时严格校验，防止路径穿越
var backupNameRe = regexp.MustCompile(`^\d{8}-\d{6}\.\d+\.ya?ml$`)

// ConfigBackup 备份文件信息
type ConfigBackup struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (c *Config) backupDir() string {
	return filepath.Join(filepath.Dir(c.path), ".backups")
}

// backupEntries 返回备份目录中的合法备份文件（按字典序，即时间序）。目录不存在时返回 nil。
func (c *Config) backupEntries() ([]os.DirEntry, error) {
	entries, err := os.ReadDir(c.backupDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []os.DirEntry
	for _, e := range entries {
		if !e.IsDir() && backupNameRe.MatchString(e.Name()) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

// backupLocked 复制当前配置文件到备份目录，仅保留最近份
func (c *Config) backupLocked() error {
	dir := c.backupDir()
	// 备份含 SSH 明文密码：目录收紧到 0700，文件 0600
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	b, err := os.ReadFile(c.path)
	if err != nil {
		return err
	}
	name := time.Now().Format("20060102-150405.000000000") + ".yaml"
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
		return err
	}
	entries, err := c.backupEntries()
	if err != nil {
		return err
	}
	// 文件名即时间戳，字典序即时间序，删掉最旧的
	for _, old := range entries[:max(0, len(entries)-maxConfigBackups)] {
		_ = os.Remove(filepath.Join(dir, old.Name()))
	}
	return nil
}

// Backups 列出可恢复的配置备份（按时间倒序）
func (c *Config) Backups() ([]ConfigBackup, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entries, err := c.backupEntries()
	if err != nil {
		return nil, err
	}
	out := make([]ConfigBackup, 0, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		var size int64
		if info, err := entries[i].Info(); err == nil {
			size = info.Size()
		}
		out = append(out, ConfigBackup{Name: entries[i].Name(), Size: size})
	}
	return out, nil
}

// Restore 恢复指定备份：先备份当前版本，再用备份覆盖并重载，支持反复回滚
func (c *Config) Restore(name string) error {
	if !backupNameRe.MatchString(name) {
		return fmt.Errorf("备份名称不合法: %q", name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// 恢复前同样尽量备份当前版本，备份失败不阻断恢复
	_ = c.backupLocked()
	b, err := os.ReadFile(filepath.Join(c.backupDir(), name))
	if err != nil {
		return fmt.Errorf("读取备份失败: %w", err)
	}
	fresh := &Config{}
	if err := yaml.Unmarshal(b, fresh); err != nil {
		return fmt.Errorf("备份文件损坏: %w", err)
	}
	fresh.path = c.path
	fresh.normalize()
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return err
	}
	c.Server = fresh.Server
	c.SSHServers = fresh.SSHServers
	c.Apps = fresh.Apps
	return nil
}

// ExpandHome 展开开头的 ~ 为当前用户主目录
func ExpandHome(p string) string { return expandHome(p) }

func expandHome(p string) string {
	p = strings.TrimSpace(p)
	if p == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
	}
	if strings.HasPrefix(p, "~/") || strings.HasPrefix(p, "~\\") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, p[2:])
		}
	}
	return p
}

// ResolveLocalProjectDir 返回展开 ~ 后的本地项目目录路径
func (a *AppConfig) ResolveLocalProjectDir() string {
	return expandHome(a.LocalProjectDir)
}

// ResolveLocalArtifactPath 返回展开 ~ 且结合 LocalProjectDir 解析后的本地默认产物绝对路径
func (a *AppConfig) ResolveLocalArtifactPath() string {
	p := expandHome(a.LocalArtifactPath)
	if p == "" {
		return ""
	}
	// 先展开 ~ 再判断绝对路径，避免 "~/x" 被当成相对路径拼到项目目录下
	if !filepath.IsAbs(p) && strings.TrimSpace(a.LocalProjectDir) != "" {
		return filepath.Join(a.ResolveLocalProjectDir(), p)
	}
	return p
}
