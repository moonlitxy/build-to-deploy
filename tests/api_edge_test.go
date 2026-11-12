package tests

import (
	"build-to-deploy/internal/api"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 这些用例集中覆盖各接口的"输入非法 / 资源不存在 / 凭据掩码"分支

func TestAPIEdgeSaveServerValidationAndMasking(t *testing.T) {
	srv, cfg, _, _ := newTestServer(t)
	h := srv.Handler()

	cases := []struct {
		name string
		body string
		code int
	}{
		{"缺 host/user", `{"host":"","user":""}`, 400},
		{"端口越界", `{"host":"1.2.3.4","user":"root","port":70000}`, 400},
		{"host 非法", `{"host":"bad host;rm","user":"root"}`, 400},
		{"user 非法", `{"host":"1.2.3.4","user":"ro ot"}`, 400},
		{"认证方式非法", `{"host":"1.2.3.4","user":"root","authType":"telnet"}`, 400},
	}
	for _, c := range cases {
		if rec := do(t, h, "POST", "/api/servers", c.body, nil); rec.Code != c.code {
			t.Fatalf("%s: 期望 %d，实际 %d %s", c.name, c.code, rec.Code, rec.Body.String())
		}
	}

	// 新建服务器：自动生成 ID、端口默认 22、返回口令掩码
	var created map[string]any
	rec := do(t, h, "POST", "/api/servers", `{"name":"n","host":"10.1.1.1","user":"root","password":"secret"}`, nil)
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	if err := dec.Decode(&created); err != nil {
		t.Fatalf("响应解析失败: %v / %s", err, rec.Body.String())
	}
	if created["id"] == nil || created["password"] != api.MaskedValueForTest {
		t.Fatalf("新建服务器返回 = %v", created)
	}

	// 编辑时回传掩码：保留原口令
	srvID, _ := created["id"].(string)
	body := `{"id":"` + srvID + `","name":"n","host":"10.1.1.1","user":"root","password":"` + api.MaskedValueForTest + `"}`
	if rec = do(t, h, "POST", "/api/servers", body, nil); rec.Code != 200 {
		t.Fatalf("编辑服务器 = %d %s", rec.Code, rec.Body.String())
	}
	if got := cfg.FindServer(srvID).Password; got != "secret" {
		t.Fatalf("口令不应被掩码覆盖: %q", got)
	}
	// 留空口令同样保留原值
	body = `{"id":"` + srvID + `","name":"n","host":"10.1.1.1","user":"root","password":""}`
	do(t, h, "POST", "/api/servers", body, nil)
	if got := cfg.FindServer(srvID).Password; got != "secret" {
		t.Fatalf("空口令不应覆盖原值: %q", got)
	}
	// 显式传入新口令则覆盖
	body = `{"id":"` + srvID + `","name":"n","host":"10.1.1.1","user":"root","password":"new"}`
	do(t, h, "POST", "/api/servers", body, nil)
	if got := cfg.FindServer(srvID).Password; got != "new" {
		t.Fatalf("新口令应生效: %q", got)
	}

	if rec = do(t, h, "POST", "/api/servers", `not-json`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
}

func TestAPIEdgeTestServerCredentialMerge(t *testing.T) {
	srv, cfg, _, _ := newTestServer(t)
	h := srv.Handler()
	app := *cfg.FindServer("s1")
	app.Password = "stored"
	cfg.UpdateServer(&app)

	// 掩码凭据不能被当成真实密码：返回的掩码值在保存/测试时保留已保存凭据，不会拿 ****** 去连
	if !api.NewServerForTest(srv).HasPool() {
		t.Skip("该测试服务器未接入 SSH 连接池，无法真实拨号")
	}
	port, closeFn := newSessionSSHServer(t, sessionBehavior{stdout: "Linux"})
	defer closeFn()
	rec := do(t, h, "POST", "/api/servers", fmt.Sprintf(`{"id":"s2","name":"fake","host":"127.0.0.1","port":%d,"user":"root","authType":"password","password":"x"}`, port), nil)
	if rec.Code != 200 {
		t.Fatalf("保存假服务器 = %d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, h, "POST", "/api/servers/test", fmt.Sprintf(`{"id":"s2","host":"127.0.0.1","port":%d,"user":"root","authType":"password","password":"%s"}`, port, api.MaskedValueForTest), nil)
	var res map[string]string
	decode(t, rec, &res)
	if rec.Code != 200 {
		t.Fatalf("掩码凭据测试连接 = %d %v", rec.Code, res)
	}
	if rec = do(t, h, "POST", "/api/servers/test", `bad`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
}

func TestAPIEdgeAppAndConfigEdgeCases(t *testing.T) {
	srv, cfg, _, _ := newTestServer(t)
	h := srv.Handler()

	if rec := do(t, h, "POST", "/api/apps", `{"name":""}`, nil); rec.Code != 400 {
		t.Fatalf("空名称应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/apps", `bad`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
	// ID 为空时取名称
	rec := do(t, h, "POST", "/api/apps", `{"name":"by-name","serverId":"s1","image":"a/b","artifactDir":"/x","buildDir":"/y"}`, nil)
	if rec.Code != 200 || cfg.FindApp("by-name") == nil {
		t.Fatalf("按名称生成 ID 失败: %d %s", rec.Code, rec.Body.String())
	}
	if rec = do(t, h, "DELETE", "/api/apps/bad%24id", "", nil); rec.Code != 400 {
		t.Fatalf("非法应用 ID 应 400，实际 %d", rec.Code)
	}

	// 备份列表：至少有一份备份（保存应用时会自动备份）
	var backups []configBackupAlias
	decode(t, do(t, h, "GET", "/api/config/backups", "", nil), &backups)
	if len(backups) == 0 {
		t.Fatal("应有配置备份")
	}
	if rec = do(t, h, "POST", "/api/config/restore", `{"name":"`+backups[0].Name+`"}`, nil); rec.Code != 200 {
		t.Fatalf("恢复备份 = %d %s", rec.Code, rec.Body.String())
	}
	if rec = do(t, h, "POST", "/api/config/restore", `bad`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
}

func TestAPIEdgeUploadEdgeCases(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()

	if rec := do(t, h, "POST", "/api/uploads", `{"fileName":"a.jar","size":0}`, nil); rec.Code != 400 {
		t.Fatalf("大小为 0 应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/uploads", `bad`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/uploads/missing", "", nil); rec.Code != 404 {
		t.Fatalf("不存在的上传应 404，实际 %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/uploads/bad%24id", "", nil); rec.Code != 400 {
		t.Fatalf("非法上传 ID 应 400，实际 %d", rec.Code)
	}
	// 写分片：offset 非法 / 任务不存在 / 未收全即完成
	if rec := doNilBody(t, h, "PUT", "/api/uploads/missing?offset=-1"); rec.Code != 400 {
		t.Fatalf("非法 offset 应 400，实际 %d", rec.Code)
	}
	if rec := doNilBody(t, h, "PUT", "/api/uploads/missing?offset=0"); rec.Code != 404 {
		t.Fatalf("不存在的上传写分片应 404，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/uploads/missing/complete", "", nil); rec.Code != 400 {
		t.Fatalf("完成不存在的上传应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/uploads/bad%24id/complete", "", nil); rec.Code != 400 {
		t.Fatalf("非法上传 ID 应 400，实际 %d", rec.Code)
	}
}

func TestAPIEdgeStartReleaseInputValidation(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()
	// 非法 JSON
	if rec := do(t, h, "POST", "/api/releases", `bad`, nil); rec.Code != 400 {
		t.Fatalf("非法 JSON 应 400，实际 %d", rec.Code)
	}
	// 含控制字符的本地产物路径
	if rec := do(t, h, "POST", "/api/releases", "{\"appId\":\"demo\",\"tag\":\"v1\",\"localArtifactPath\":\"a\\u0000b\"}", nil); rec.Code != 400 {
		t.Fatalf("控制字符路径应 400，实际 %d", rec.Code)
	}
	// 相对路径
	if rec := do(t, h, "POST", "/api/releases", `{"appId":"demo","tag":"v1","localArtifactPath":"rel/a.jar"}`, nil); rec.Code != 400 {
		t.Fatalf("相对路径应 400，实际 %d", rec.Code)
	}
	// 未知应用（路径校验通过后进入引擎才报错）
	if rec := do(t, h, "POST", "/api/releases", `{"appId":"nope","tag":"v1"}`, nil); rec.Code != 400 {
		t.Fatalf("未知应用应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "POST", "/api/releases/bad%24id/cancel", "", nil); rec.Code != 400 {
		t.Fatalf("非法发布 ID 应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/releases/bad%24id", "", nil); rec.Code != 400 {
		t.Fatalf("非法发布 ID 应 400，实际 %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/api/releases/missing", "", nil); rec.Code != 404 {
		t.Fatalf("不存在的发布应 404，实际 %d", rec.Code)
	}
}

func TestAPIEdgeGuardContentType(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()
	// 写请求用 text/plain（跨站"简单请求"的典型形态）且不带 Origin：必须拒绝
	req := httptest.NewRequest("POST", "/api/apps", strings.NewReader(`{"name":"x"}`))
	req.Host = "127.0.0.1:8080"
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden && rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("非 JSON 写请求应被拒绝，实际 %d", rec.Code)
	}
	// 同源的 text/plain 也应被拒绝（不是合法 JSON）
	req = httptest.NewRequest("POST", "/api/apps", strings.NewReader(`{"name":"x"}`))
	req.Host = "127.0.0.1:8080"
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Origin", "http://127.0.0.1:8080")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden && rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("非 JSON 内容类型应被拒绝，实际 %d", rec.Code)
	}
	// PUT 分片走 application/octet-stream，应放行
	req = httptest.NewRequest("PUT", "/api/uploads/missing?offset=0", strings.NewReader(""))
	req.Host = "127.0.0.1:8080"
	req.Header.Set("Content-Type", "application/octet-stream")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusForbidden {
		t.Fatalf("分片上传不应被内容类型拦截: %s", rec.Body.String())
	}
}

// 注意：/api/local/pick-path 会真的唤起系统文件选择框，测试中一律不调用。
// "同一时刻只允许一个对话框"由 Server 内部的 pathPickerMu 保证，随接口实现一起维护。

func doNilBody(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, method, path, "", nil)
}

type configBackupAlias struct {
	Name string `json:"name"`
}
