package tests

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"build-to-deploy/internal/api"
	"build-to-deploy/internal/store"
)

// 镜像维护接口：只删用户勾选且未受保护/未被引用的镜像
func TestAPIDockerImagesMaintenance(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	h := srv.Handler()
	st := api.NewServerForTest(srv).Engine().Store

	// 最近一次发布版本 → 受保护；线上运行版本 v0.9.0 → 受保护
	st.Add(&store.Release{ID: "20261114000000-aaaaaaaa", App: "demo", Tag: "v1.0.0",
		Image: "app/demo:v1.0.0", Status: store.StatusSuccess, CreatedAt: time.Now()})

	fake.outputs = map[string]string{
		// 列表命令须排在前面：docker image inspect 是 docker images 的子串之外，互不冲突
		"docker images --no-trunc --format": strings.Join([]string{
			"app/demo:v1.0.0|sha256:1|100MB|2026-11-14 10:00:00 +0800 CST",
			"app/demo:v0.9.0|sha256:2|90MB|2026-11-13 10:00:00 +0800 CST",
			"app/demo:v0.8.0|sha256:3|80MB|2026-11-12 10:00:00 +0800 CST",
			"app/demo:v0.7.0|sha256:4|70MB|2026-11-11 10:00:00 +0800 CST",
		}, "\n"),
		// 容器引用：v0.7.0 被一个已停止的容器使用
		"docker ps -a": "app/demo:v0.7.0\n",
		// K8s 线上镜像
		"get deployment": "app/demo:v0.9.0",
	}

	rec := do(t, h, "GET", "/api/docker/images?appId=demo", "", nil)
	if rec.Code != 200 {
		t.Fatalf("列表 = %d %s", rec.Code, rec.Body.String())
	}
	var list struct {
		Images []struct {
			Tag             string `json:"tag"`
			InUse           bool   `json:"inUse"`
			Protected       bool   `json:"protected"`
			ProtectedReason string `json:"protectedReason"`
		} `json:"images"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	state := map[string]string{}
	for _, img := range list.Images {
		switch {
		case img.Protected:
			state[img.Tag] = "protected:" + img.ProtectedReason
		case img.InUse:
			state[img.Tag] = "in-use"
		default:
			state[img.Tag] = "deletable"
		}
	}
	if len(state) != 4 {
		t.Fatalf("应列出 4 个镜像: %+v", state)
	}
	for tag, want := range map[string]string{
		"v1.0.0": "protected:最近一次发布的版本",
		"v0.9.0": "protected:线上正在运行的版本",
		"v0.8.0": "deletable",
		"v0.7.0": "in-use",
	} {
		if state[tag] != want {
			t.Fatalf("%s 状态 = %q，期望 %q（全部：%+v）", tag, state[tag], want, state)
		}
	}

	// 混选：只删除可删除的 v0.8.0，其余跳过
	rec = do(t, h, "POST", "/api/docker/images/delete",
		`{"appId":"demo","refs":["app/demo:v0.8.0","app/demo:v1.0.0","app/demo:v0.9.0","app/demo:v0.7.0"]}`, nil)
	if rec.Code != 200 {
		t.Fatalf("删除 = %d %s", rec.Code, rec.Body.String())
	}
	var del struct {
		Removed []string `json:"removed"`
		Skipped []string `json:"skipped"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &del)
	if len(del.Removed) != 1 || del.Removed[0] != "app/demo:v0.8.0" {
		t.Fatalf("只应删除 v0.8.0: %+v", del)
	}
	if len(del.Skipped) != 3 {
		t.Fatalf("应跳过 3 个: %+v", del)
	}
	var rmCmd string
	for _, c := range fake.cmds {
		if strings.HasPrefix(c, "docker image rm ") {
			rmCmd = c
		}
	}
	if !strings.Contains(rmCmd, "'app/demo:v0.8.0'") || strings.Contains(rmCmd, "v0.9.0") || strings.Contains(rmCmd, "v1.0.0") {
		t.Fatalf("删除命令不正确: %q", rmCmd)
	}

	// 非法输入
	if rec := do(t, h, "POST", "/api/docker/images/delete", `{"appId":"demo","refs":[]}`, nil); rec.Code != 400 {
		t.Fatalf("空列表应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/docker/images/delete", `{"appId":"demo","refs":["other/app:v1"]}`, nil); rec.Code != 400 {
		t.Fatalf("跨仓库引用应 400，实际 %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, "POST", "/api/docker/images/delete", `{"appId":"nope","refs":["app/demo:v0.8.0"]}`, nil); rec.Code != 404 {
		t.Fatalf("未知应用应 404，实际 %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/docker/images?appId=nope", "", nil); rec.Code != 404 {
		t.Fatalf("未知应用列表应 404，实际 %d", rec.Code)
	}

	// 未连接服务器时拒绝
	srv.SetConnectionEnabled(false)
	if rec := do(t, h, "GET", "/api/docker/images?appId=demo", "", nil); rec.Code != 412 {
		t.Fatalf("未连接时列表应 412，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/docker/images/delete", `{"appId":"demo","refs":["app/demo:v0.8.0"]}`, nil); rec.Code != 412 {
		t.Fatalf("未连接时删除应 412，实际 %d", rec.Code)
	}
}

// 删除命令失败时返回 500，并把远程输出带回页面
func TestAPIDockerImagesDeleteCommandFailure(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	h := srv.Handler()
	fake.outputs = map[string]string{
		"docker images --no-trunc --format": "app/demo:v0.1.0|sha256:1|1MB|x",
		"docker ps -a":                      "",
	}
	fake.runErr = errors.New("exit status 1")
	fake.runOut = "Error response from daemon: conflict: image is being used"
	rec := do(t, h, "POST", "/api/docker/images/delete", `{"appId":"demo","refs":["app/demo:v0.1.0"]}`, nil)
	if rec.Code != 500 {
		t.Fatalf("删除失败应 500，实际 %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "image is being used") {
		t.Fatalf("应带回远程输出: %s", rec.Body.String())
	}
	// 超长输出会被截断
	fake.runOut = strings.Repeat("x", 800)
	rec = do(t, h, "POST", "/api/docker/images/delete", `{"appId":"demo","refs":["app/demo:v0.1.0"]}`, nil)
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), "...") {
		t.Fatalf("超长输出应截断: %d", rec.Code)
	}
}

// 无法确认容器引用关系时必须取消删除，避免误删在用镜像
func TestAPIDockerImagesDeleteAbortsWhenUsageUnknown(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	h := srv.Handler()
	fake.outputs = map[string]string{
		"docker images --no-trunc --format": "app/demo:v0.1.0|sha256:1|1MB|x",
	}
	fake.err = errors.New("ssh 断开")
	rec := do(t, h, "POST", "/api/docker/images/delete", `{"appId":"demo","refs":["app/demo:v0.1.0"]}`, nil)
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), "已取消删除") {
		t.Fatalf("引用关系未知时应取消删除: %d %s", rec.Code, rec.Body.String())
	}
	for _, c := range fake.cmds {
		if strings.HasPrefix(c, "docker image rm ") {
			t.Fatalf("不应执行删除: %s", c)
		}
	}
}

// 所有成功发布过的历史版本都受保护，避免用户手动清理掉回滚依赖的镜像
func TestAPIDockerImagesProtectsHistory(t *testing.T) {
	srv, _, fake, _ := newTestServer(t)
	h := srv.Handler()
	st := api.NewServerForTest(srv).Engine().Store
	now := time.Now()
	for i, tag := range []string{"v1.0.0", "v0.9.0", "v0.8.0"} {
		st.Add(&store.Release{ID: "h" + tag, App: "demo", Tag: tag, Image: "app/demo:" + tag,
			Status: store.StatusSuccess, CreatedAt: now.Add(-time.Duration(i) * time.Minute)})
	}
	// 失败的发布不保护：它的镜像并没有被线上使用过
	st.Add(&store.Release{ID: "xfail", App: "demo", Tag: "v0.7.0", Image: "app/demo:v0.7.0",
		Status: store.StatusFailed, CreatedAt: now.Add(-time.Hour)})

	fake.outputs = map[string]string{
		"docker images --no-trunc --format": strings.Join([]string{
			"app/demo:v1.0.0|sha256:1|1MB|x",
			"app/demo:v0.9.0|sha256:2|1MB|x",
			"app/demo:v0.8.0|sha256:3|1MB|x",
			"app/demo:v0.7.0|sha256:4|1MB|x",
		}, "\n"),
		"docker ps -a":   "",
		"get deployment": "app/demo:v1.0.0",
	}
	rec := do(t, h, "POST", "/api/docker/images/delete",
		`{"appId":"demo","refs":["app/demo:v1.0.0","app/demo:v0.9.0","app/demo:v0.8.0","app/demo:v0.7.0","app/demo:v0.7.0"]}`, nil)
	if rec.Code != 200 {
		t.Fatalf("删除 = %d %s", rec.Code, rec.Body.String())
	}
	var del struct {
		Removed []string `json:"removed"`
		Skipped []string `json:"skipped"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &del)
	// 只有失败记录对应的 v0.7.0 可删，且重复提交只删一次
	if len(del.Removed) != 1 || del.Removed[0] != "app/demo:v0.7.0" {
		t.Fatalf("只应删除未被发布使用过的 v0.7.0（且去重）: %+v", del)
	}
	if len(del.Skipped) != 3 {
		t.Fatalf("应跳过 3 个历史版本: %+v", del.Skipped)
	}
	for _, s := range del.Skipped {
		if !strings.Contains(s, "回滚依赖") && !strings.Contains(s, "最近一次发布") && !strings.Contains(s, "线上正在运行") {
			t.Fatalf("跳过原因应说明是发布过的版本: %s", s)
		}
	}
}
