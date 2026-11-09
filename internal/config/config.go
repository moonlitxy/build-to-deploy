package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

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

	// 目录分离配置（满足需求 2）
	ArtifactDir string `yaml:"artifactDir" json:"artifactDir"` // 产物单独目录，如 /data/artifacts/order-service
	BuildDir    string `yaml:"buildDir" json:"buildDir"`       // 镜像构建单独目录，如 /data/docker-build/order-service
	K8sYamlDir  string `yaml:"k8sYamlDir" json:"k8sYamlDir"`   // K8s YAML 配置单独目录，如 /data/k8s/order-service

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

var TagRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$`)

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
			Listen:      ":8080",
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
				ID:          "order-service",
				Name:        "order-service",
				Description: "订单服务",
				ServerID:    "default-linux",
				Type:        "jar",
				ArtifactDir: "/data/artifacts/order-service",
				BuildDir:    "/data/docker-build/order-service",
				K8sYamlDir:  "/data/k8s/order-service",
				Image:       "harbor.example.com/app/order-service",
				BuildCmd:    "cd {{.BuildDir}} && docker build -t {{.Image}}:{{.Tag}} .",
				PushCmd:     "docker push {{.Image}}:{{.Tag}}",
				DeployCmd:   "kubectl -n {{.Namespace}} set image deployment/{{.Deployment}} {{.Container}}={{.Image}}:{{.Tag}}",
				Namespace:   "prod",
				Deployment:  "order-service",
				Container:   "order-service",
				RolloutTimeout: 300,
				AutoRollback:   true,
			},
		},
	}
}

func (c *Config) normalize() {
	if c.Server.Listen == "" {
		c.Server.Listen = ":8080"
	}
	if c.Server.DataDir == "" {
		c.Server.DataDir = "./data"
	}
	if c.Server.MaxUploadMB <= 0 {
		c.Server.MaxUploadMB = 2048
	}

	for _, app := range c.Apps {
		if app.ID == "" {
			app.ID = app.Name
		}
		if app.BuildCmd == "" {
			app.BuildCmd = "cd {{.BuildDir}} && docker build -t {{.Image}}:{{.Tag}} ."
		}
		if app.DeployCmd == "" {
			if app.K8sYamlDir != "" {
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
		// 规范化路径统一为 linux 斜杠
		app.ArtifactDir = strings.TrimRight(filepath.ToSlash(app.ArtifactDir), "/")
		app.BuildDir = strings.TrimRight(filepath.ToSlash(app.BuildDir), "/")
		app.K8sYamlDir = strings.TrimRight(filepath.ToSlash(app.K8sYamlDir), "/")
	}
}

func (c *Config) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	if c.path == "" {
		c.path = "config.yaml"
	}
	return os.WriteFile(c.path, b, 0644)
}

func (c *Config) FindApp(idOrName string) *AppConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, a := range c.Apps {
		if a.ID == idOrName || a.Name == idOrName {
			return a
		}
	}
	return nil
}

func (c *Config) FindServer(id string) *sshpool.ServerConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.SSHServers {
		if s.ID == id {
			return s
		}
	}
	if len(c.SSHServers) > 0 {
		return c.SSHServers[0]
	}
	return nil
}

func (c *Config) UpdateServer(s *sshpool.ServerConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	found := false
	for i, old := range c.SSHServers {
		if old.ID == s.ID {
			c.SSHServers[i] = s
			found = true
			break
		}
	}
	if !found {
		c.SSHServers = append(c.SSHServers, s)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	found := false
	for i, old := range c.Apps {
		if old.ID == app.ID {
			c.Apps[i] = app
			found = true
			break
		}
	}
	if !found {
		c.Apps = append(c.Apps, app)
	}
	return c.saveUnlocked()
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
		c.path = "config.yaml"
	}
	return os.WriteFile(c.path, b, 0644)
}
