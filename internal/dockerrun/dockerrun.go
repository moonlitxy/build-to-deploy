// Package dockerrun 生成宿主机 Docker 容器部署（deployMode=docker）所需的远程命令。
//
// 与 K8s 形态的区别：镜像直接用 docker run 在宿主机起容器，不依赖 kubectl / YAML。
// 这里只负责"拼命令"，执行统一走 sshpool 长连接；所有字符串都来自已校验的配置或镜像
// 引用（config.ValidateDocker / ImageRe / imageRefRe），拼进 shell 前一律单引号转义。
package dockerrun

import (
	"errors"
	"fmt"
	"strings"

	"build-to-deploy/internal/config"
)

// 容器不存在 / 正在运行专用的查询命令
const (
	// InspectCmd 打印容器状态与镜像；容器不存在时输出空，不报错
	InspectCmd = `docker inspect -f '{{.State.Status}} {{.Config.Image}}' <name> 2>/dev/null || true`
	// ExistsCmd 仅判断容器是否存在（返回容器 ID 或空）
	ExistsCmd = `docker container inspect -f '{{.Id}}' <name> 2>/dev/null || true`
)

// Quote 单引号转义，供远程 sh 使用
func Quote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// Validate 校验 Docker 部署所需字段：容器名必填，其余走白名单
func Validate(app *config.AppConfig) error {
	if strings.TrimSpace(app.DockerContainer) == "" {
		return errors.New("Docker 部署形态需要先填写容器名 (dockerContainer)")
	}
	return config.ValidateDocker(normalize(app))
}

// RunArgs 生成 docker run 的参数（不含镜像名），顺序固定便于测试与日志阅读。
// 卷映射与环境变量按逗号拆分，每项作为独立参数，避免整串被 shell 当成一个值。
func RunArgs(app *config.AppConfig) ([]string, error) {
	if err := Validate(app); err != nil {
		return nil, err
	}
	args := []string{"-d", "--name", app.DockerContainer}
	app = normalize(app)
	if p := strings.TrimSpace(app.DockerPort); p != "" {
		args = append(args, "-p", p)
	}
	if v := strings.TrimSpace(app.DockerVolumes); v != "" {
		for _, item := range strings.Split(v, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			args = append(args, "-v", item)
		}
	}
	if e := strings.TrimSpace(app.DockerEnv); e != "" {
		for _, item := range strings.Split(e, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			args = append(args, "-e", item)
		}
	}
	if o := strings.TrimSpace(app.DockerRunOpts); o != "" {
		args = append(args, strings.Fields(o)...)
	}
	return args, nil
}

// normalize 返回去空白后的配置副本，校验与拼命令都用同一份值。
// 逗号分隔的列表允许人工输入时带空格，统一去掉每项首尾空白后再校验
func normalize(app *config.AppConfig) *config.AppConfig {
	trimList := func(v string) string {
		parts := strings.Split(v, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return strings.Trim(strings.Join(parts, ","), ",")
	}
	cp := *app
	cp.DockerVolumes = trimList(cp.DockerVolumes)
	cp.DockerEnv = trimList(cp.DockerEnv)
	cp.DockerRunOpts = strings.Join(strings.Fields(cp.DockerRunOpts), " ")
	cp.DockerPort = strings.TrimSpace(cp.DockerPort)
	cp.DockerHealthPath = strings.TrimSpace(cp.DockerHealthPath)
	cp.DockerContainer = strings.TrimSpace(cp.DockerContainer)
	return &cp
}

// RunCmd 生成完整的 docker run 命令：先移除同名容器，再以新镜像启动
func RunCmd(app *config.AppConfig, imageRef string) (string, error) {
	if !looksLikeImageRef(imageRef) {
		return "", fmt.Errorf("镜像引用不合法: %q", imageRef)
	}
	args, err := RunArgs(app)
	if err != nil {
		return "", err
	}
	for i, a := range args {
		args[i] = Quote(a)
	}
	return fmt.Sprintf("docker rm -f %s >/dev/null 2>&1 || true; docker run %s %s",
		Quote(app.DockerContainer), strings.Join(args, " "), Quote(imageRef)), nil
}

// InspectCmdFor 生成查询容器状态的命令
func InspectCmdFor(app *config.AppConfig) (string, error) {
	if err := Validate(app); err != nil {
		return "", err
	}
	return strings.ReplaceAll(InspectCmd, "<name>", Quote(app.DockerContainer)), nil
}

// ParseInspect 解析 InspectCmdFor 的输出：status 为容器状态，image 为容器当前使用的镜像
func ParseInspect(out string) (status, image string) {
	fields := strings.Fields(strings.TrimSpace(out))
	if len(fields) == 0 {
		return "", ""
	}
	status = fields[0]
	if len(fields) > 1 {
		image = fields[1]
	}
	return status, image
}

// SameImage 判断容器记录的镜像与目标镜像是否是同一个 Tag：新版 docker 存的是镜像 ID，
// 只要调用方用镜像 ID 比较就能匹配；旧版或不带 ID 时回退到字符串比较
func SameImage(current, target, id string) bool {
	if current == "" || target == "" {
		return false
	}
	if id != "" && current == id {
		return true
	}
	return current == target
}

// HealthCmd 生成容器内健康探测命令：有路径时用 wget（容器内通常没有 curl），否则只检查状态
func HealthCmd(app *config.AppConfig) string {
	path := strings.TrimSpace(app.DockerHealthPath)
	if path == "" {
		return ""
	}
	base := "http://127.0.0.1"
	if p := strings.TrimSpace(app.DockerPort); p != "" {
		// 端口映射形如 宿主机:容器；容器内访问应使用容器端口
		parts := strings.Split(p, ":")
		base += ":" + parts[len(parts)-1]
	}
	return fmt.Sprintf("wget -q -T 5 -O /dev/null %s", Quote(base+path))
}

// ReadyCmd 生成"等待容器进入 running 且健康检查通过"的远程命令。
// 用 sh 循环在服务器上完成等待，避免本端反复 SSH；输出 READY / FAILED / TIMEOUT 供调用方判断。
func ReadyCmd(app *config.AppConfig, timeoutSec int) (string, error) {
	insp, err := InspectCmdFor(app)
	if err != nil {
		return "", err
	}
	if timeoutSec <= 0 {
		timeoutSec = 300
	}
	health := ""
	if h := HealthCmd(app); h != "" {
		health = fmt.Sprintf("docker exec %s sh -c %s >/dev/null 2>&1", Quote(app.DockerContainer), Quote(h))
	}
	loop := fmt.Sprintf(`i=0; while [ $i -lt %[1]d ]; do s=$(%[2]s); case "$s" in running*) %[3]s && echo READY && exit 0;; exited*|dead*) echo "$s"; echo FAILED; exit 1;; esac; i=$((i+1)); sleep 3; done; echo "$s"; echo TIMEOUT; exit 1`,
		timeoutSec/3+1, insp, health)
	if health == "" {
		// 没有健康检查时只等 running：把三个条件串里的最后一个空条件去掉
		loop = fmt.Sprintf(`i=0; while [ $i -lt %[1]d ]; do s=$(%[2]s); case "$s" in running*) echo READY && exit 0;; exited*|dead*) echo "$s"; echo FAILED; exit 1;; esac; i=$((i+1)); sleep 3; done; echo "$s"; echo TIMEOUT; exit 1`,
			timeoutSec/3+1, insp)
	}
	return loop, nil
}

func looksLikeImageRef(ref string) bool {
	if ref == "" || len(ref) > 255 {
		return false
	}
	if strings.ContainsAny(ref, " \t\n'\";$&|`\\><") {
		return false
	}
	return true
}
