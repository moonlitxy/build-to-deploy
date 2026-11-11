package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"build-to-deploy/internal/sshpool"
)

// k8sNameRe K8s 资源名（RFC 1123 label）。这些值会拼接进远程 shell 命令，必须白名单校验
var k8sNameRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// maxTailLines 日志最大行数，防止一次性拉取海量日志耗尽本机内存
const maxTailLines = 4000

// CheckName 校验 K8s 资源名（RFC 1123 label）：API 层用它做 400 校验，k8s 层用它守远程命令拼接
func CheckName(kind, v string) error {
	if !k8sNameRe.MatchString(v) {
		return fmt.Errorf("%s 名称不合法: %q（只允许小写字母、数字、-，且不超过 63 个字符）", kind, v)
	}
	return nil
}

type PodInfo struct {
	Name       string   `json:"name"`
	Namespace  string   `json:"namespace"`
	Status     string   `json:"status"`
	Ready      string   `json:"ready"`
	Restarts   int32    `json:"restarts"`
	Age        string   `json:"age"`
	IP         string   `json:"ip"`
	Node       string   `json:"node"`
	Containers []string `json:"containers"`
	Images     []string `json:"images"`
}

type Client struct {
	Pool *sshpool.Pool
	// dryRun 每次调用实时读取配置，与 pipeline.Engine 保持同一来源：
	// 否则改配置后引擎真实执行、K8s 仍走模拟，会出现“回滚成功但实际没回滚”
	dryRun func() bool
}

func (c *Client) isDryRun() bool { return c.dryRun != nil && c.dryRun() }

func NewClient(pool *sshpool.Pool, dryRun func() bool) *Client {
	return &Client{
		Pool:   pool,
		dryRun: dryRun,
	}
}

// ListNamespaces 获取集群命名空间列表
func (c *Client) ListNamespaces(ctx context.Context, srv *sshpool.ServerConfig) ([]string, error) {
	if c.isDryRun() {
		return []string{"default", "prod", "test", "kube-system"}, nil
	}
	out, err := c.Pool.ExecuteOutput(ctx, srv, "kubectl get namespaces -o jsonpath='{.items[*].metadata.name}'")
	if err != nil {
		return nil, fmt.Errorf("获取命名空间失败: %w", err)
	}
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return []string{"default"}, nil
	}
	return fields, nil
}

// ListPods 获取指定命名空间下的所有 Pod 及状态
func (c *Client) ListPods(ctx context.Context, srv *sshpool.ServerConfig, namespace string) ([]PodInfo, error) {
	if namespace == "" {
		namespace = "default"
	}
	// 命名空间校验放在 DryRun 分支之前，保证演练与真实模式行为一致
	if err := CheckName("命名空间", namespace); err != nil {
		return nil, err
	}
	if c.isDryRun() {
		return []PodInfo{
			{
				Name:       "order-service-7d8b5c968-abc12",
				Namespace:  namespace,
				Status:     "Running",
				Ready:      "1/1",
				Restarts:   0,
				Age:        "12m",
				IP:         "10.244.1.34",
				Node:       "node-worker-01",
				Containers: []string{"order-service"},
				Images:     []string{"harbor.example.com/app/order-service:v1.0.0"},
			},
			{
				Name:       "order-service-7d8b5c968-xyz89",
				Namespace:  namespace,
				Status:     "Running",
				Ready:      "1/1",
				Restarts:   0,
				Age:        "12m",
				IP:         "10.244.2.56",
				Node:       "node-worker-02",
				Containers: []string{"order-service"},
				Images:     []string{"harbor.example.com/app/order-service:v1.0.0"},
			},
		}, nil
	}

	cmd := fmt.Sprintf("kubectl get pods -n %s -o json", sshpool.ShellQuote(namespace))
	out, err := c.Pool.ExecuteOutput(ctx, srv, cmd)
	if err != nil {
		return nil, fmt.Errorf("查询 Pod 列表失败: %w", err)
	}

	var podList struct {
		Items []struct {
			Metadata struct {
				Name              string    `json:"name"`
				Namespace         string    `json:"namespace"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
			} `json:"metadata"`
			Spec struct {
				NodeName   string `json:"nodeName"`
				Containers []struct {
					Name  string `json:"name"`
					Image string `json:"image"`
				} `json:"containers"`
			} `json:"spec"`
			Status struct {
				Phase             string `json:"phase"`
				PodIP             string `json:"podIP"`
				ContainerStatuses []struct {
					Name         string `json:"name"`
					Ready        bool   `json:"ready"`
					RestartCount int32  `json:"restartCount"`
					State        struct {
						Waiting *struct {
							Reason string `json:"reason"`
						} `json:"waiting"`
					} `json:"state"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}

	if err := json.Unmarshal([]byte(out), &podList); err != nil {
		return nil, fmt.Errorf("解析 Pod JSON 失败: %w", err)
	}

	var res []PodInfo
	now := time.Now()

	for _, item := range podList.Items {
		totalContainers := len(item.Spec.Containers)
		readyContainers := 0
		var restarts int32 = 0
		status := item.Status.Phase

		for _, cs := range item.Status.ContainerStatuses {
			if cs.Ready {
				readyContainers++
			}
			restarts += cs.RestartCount
			if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
				status = cs.State.Waiting.Reason
			}
		}

		var containers []string
		var images []string
		for _, c := range item.Spec.Containers {
			containers = append(containers, c.Name)
			images = append(images, c.Image)
		}

		dur := now.Sub(item.Metadata.CreationTimestamp)
		age := formatDuration(dur)

		res = append(res, PodInfo{
			Name:       item.Metadata.Name,
			Namespace:  item.Metadata.Namespace,
			Status:     status,
			Ready:      fmt.Sprintf("%d/%d", readyContainers, totalContainers),
			Restarts:   restarts,
			Age:        age,
			IP:         item.Status.PodIP,
			Node:       item.Spec.NodeName,
			Containers: containers,
			Images:     images,
		})
	}

	return res, nil
}

// GetPodLogs 获取 Pod 容器日志
func (c *Client) GetPodLogs(ctx context.Context, srv *sshpool.ServerConfig, namespace, pod, container string, tailLines int) (string, error) {
	if tailLines <= 0 {
		tailLines = 200
	}
	if tailLines > maxTailLines {
		tailLines = maxTailLines
	}
	if namespace == "" {
		namespace = "default"
	}
	if err := CheckName("命名空间", namespace); err != nil {
		return "", err
	}
	if err := CheckName("Pod", pod); err != nil {
		return "", err
	}
	if container != "" {
		if err := CheckName("容器", container); err != nil {
			return "", err
		}
	}
	if c.isDryRun() {
		return fmt.Sprintf("[dry-run] 日志示例 - Pod: %s, Namespace: %s, Container: %s\n2026-11-09T22:00:00Z INFO [main] Application started successfully\n2026-11-09T22:00:01Z INFO [http] Listening on :8080", pod, namespace, container), nil
	}

	cmd := fmt.Sprintf("kubectl logs -n %s %s --tail=%d", sshpool.ShellQuote(namespace), sshpool.ShellQuote(pod), tailLines)
	if container != "" {
		cmd += " -c " + sshpool.ShellQuote(container)
	}

	return c.Pool.ExecuteOutput(ctx, srv, cmd)
}

// RolloutStatus 等待 Deployment 滚动发布完成
func (c *Client) RolloutStatus(ctx context.Context, w io.Writer, srv *sshpool.ServerConfig, namespace, deployment string, timeoutSec int) error {
	if timeoutSec <= 0 {
		timeoutSec = 300
	}
	if namespace == "" {
		namespace = "default"
	}
	if err := CheckName("命名空间", namespace); err != nil {
		return err
	}
	if err := CheckName("Deployment", deployment); err != nil {
		return err
	}
	if c.isDryRun() {
		fmt.Fprintf(w, "[dry-run] kubectl -n %s rollout status deployment/%s --timeout=%ds\n", namespace, deployment, timeoutSec)
		for i := 1; i <= 3; i++ {
			time.Sleep(300 * time.Millisecond)
			fmt.Fprintf(w, "Waiting for deployment rollout to finish: %d of 3 updated replicas are available...\n", i)
		}
		fmt.Fprintln(w, "deployment successfully rolled out")
		return nil
	}
	cmd := fmt.Sprintf("kubectl -n %s rollout status deployment/%s --timeout=%ds", sshpool.ShellQuote(namespace), sshpool.ShellQuote(deployment), timeoutSec)
	return c.Pool.RunCommand(ctx, srv, cmd, w)
}

// RestartDeployment 执行 Deployment 滚动重启
func (c *Client) RestartDeployment(ctx context.Context, srv *sshpool.ServerConfig, namespace, deployment string) error {
	if namespace == "" {
		namespace = "default"
	}
	if err := CheckName("命名空间", namespace); err != nil {
		return err
	}
	if err := CheckName("Deployment", deployment); err != nil {
		return err
	}
	if c.isDryRun() {
		return nil
	}
	cmd := fmt.Sprintf("kubectl -n %s rollout restart deployment/%s", sshpool.ShellQuote(namespace), sshpool.ShellQuote(deployment))
	out, err := c.Pool.ExecuteOutput(ctx, srv, cmd)
	if err != nil {
		return fmt.Errorf("重启 Deployment %s 失败: %w (%s)", deployment, err, out)
	}
	return nil
}

// SetImage 切换指定 Deployment 中容器的镜像（无独立 K8s YAML 文件时用于回滚）
func (c *Client) SetImage(ctx context.Context, w io.Writer, srv *sshpool.ServerConfig, namespace, deployment, container, image string) error {
	for kind, v := range map[string]string{"命名空间": namespace, "Deployment": deployment, "容器": container} {
		if err := CheckName(kind, v); err != nil {
			return err
		}
	}
	// set image 的 container=image 参数以逗号分隔，镜像引用中不允许出现逗号或空白
	if image == "" || strings.ContainsAny(image, ", \t\r\n") {
		return fmt.Errorf("镜像引用不合法: %q", image)
	}
	cmd := fmt.Sprintf("kubectl -n %s set image deployment/%s %s=%s", sshpool.ShellQuote(namespace), sshpool.ShellQuote(deployment), sshpool.ShellQuote(container), sshpool.ShellQuote(image))
	if c.isDryRun() {
		fmt.Fprintf(w, "[dry-run] $ %s\n", cmd)
		return nil
	}
	return c.Pool.RunCommand(ctx, srv, cmd, w)
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}
