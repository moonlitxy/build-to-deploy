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

func checkName(kind, v string) error {
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
	Pool   *sshpool.Pool
	DryRun bool
}

func NewClient(pool *sshpool.Pool, dryRun bool) *Client {
	return &Client{
		Pool:   pool,
		DryRun: dryRun,
	}
}

// ListNamespaces 获取集群命名空间列表
func (c *Client) ListNamespaces(ctx context.Context, srv *sshpool.ServerConfig) ([]string, error) {
	if c.DryRun {
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
	if err := checkName("命名空间", namespace); err != nil {
		return nil, err
	}
	if c.DryRun {
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

	cmd := fmt.Sprintf("kubectl get pods -n %s -o json", namespace)
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
	if err := checkName("命名空间", namespace); err != nil {
		return "", err
	}
	if err := checkName("Pod", pod); err != nil {
		return "", err
	}
	if container != "" {
		if err := checkName("容器", container); err != nil {
			return "", err
		}
	}
	if c.DryRun {
		return fmt.Sprintf("[dry-run] 日志示例 - Pod: %s, Namespace: %s, Container: %s\n2026-11-09T22:00:00Z INFO [main] Application started successfully\n2026-11-09T22:00:01Z INFO [http] Listening on :8080", pod, namespace, container), nil
	}

	cmd := fmt.Sprintf("kubectl logs -n %s %s", namespace, pod)
	if container != "" {
		cmd += " -c " + container
	}
	cmd += fmt.Sprintf(" --tail=%d", tailLines)

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
	if err := checkName("命名空间", namespace); err != nil {
		return err
	}
	if err := checkName("Deployment", deployment); err != nil {
		return err
	}
	if c.DryRun {
		fmt.Fprintf(w, "[dry-run] kubectl -n %s rollout status deployment/%s --timeout=%ds\n", namespace, deployment, timeoutSec)
		for i := 1; i <= 3; i++ {
			time.Sleep(300 * time.Millisecond)
			fmt.Fprintf(w, "Waiting for deployment rollout to finish: %d of 3 updated replicas are available...\n", i)
		}
		fmt.Fprintln(w, "deployment successfully rolled out")
		return nil
	}
	cmd := fmt.Sprintf("kubectl -n %s rollout status deployment/%s --timeout=%ds", namespace, deployment, timeoutSec)
	return c.Pool.RunCommand(ctx, srv, cmd, w)
}

// RestartDeployment 执行 Deployment 滚动重启
func (c *Client) RestartDeployment(ctx context.Context, srv *sshpool.ServerConfig, namespace, deployment string) error {
	if namespace == "" {
		namespace = "default"
	}
	if err := checkName("命名空间", namespace); err != nil {
		return err
	}
	if err := checkName("Deployment", deployment); err != nil {
		return err
	}
	if c.DryRun {
		return nil
	}
	cmd := fmt.Sprintf("kubectl -n %s rollout restart deployment/%s", namespace, deployment)
	out, err := c.Pool.ExecuteOutput(ctx, srv, cmd)
	if err != nil {
		return fmt.Errorf("重启 Deployment %s 失败: %w (%s)", deployment, err, out)
	}
	return nil
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
