package tests

import (
	"bufio"
	"build-to-deploy/internal/api"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAPIEventsReleaseEventsReplaysHistoryAndEnds(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	h := srv.Handler()
	art := filepath.Join(t.TempDir(), "app.jar")
	os.WriteFile(art, []byte("PK\x03\x04x"), 0o644)
	rec := do(t, h, "POST", "/api/releases", fmt.Sprintf(`{"appId":"demo","tag":"v4.0.0","useLocalArtifact":true,"localArtifactPath":%q}`, art), nil)
	var rel struct {
		ID string `json:"id"`
	}
	decode(t, rec, &rel)
	waitAPIDone(t, srv, rel.ID)
	// 等待 Hub 关闭（流水线结束后会从引擎中移除），此时走"回放日志文件"分支
	time.Sleep(100 * time.Millisecond)

	ts := httptest.NewServer(h)
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"/api/releases/"+rel.ID+"/events", nil)
	req.Host = "127.0.0.1:8080"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %s", ct)
	}
	var events []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event: ") {
			events = append(events, strings.TrimPrefix(line, "event: "))
		}
	}
	joined := strings.Join(events, ",")
	if !strings.Contains(joined, "release") || !strings.Contains(joined, "end") {
		t.Fatalf("事件序列 = %s", joined)
	}

	// 不存在的发布
	rec = do(t, h, "GET", "/api/releases/missing/events", "", nil)
	if rec.Code != 404 {
		t.Fatalf("不存在发布 = %d", rec.Code)
	}
}

func TestAPIEventsReadLinesRing(t *testing.T) {
	if got := api.ReadLines("/nonexistent/file"); got != nil {
		t.Fatal("不存在文件应返回 nil")
	}
	path := filepath.Join(t.TempDir(), "x.log")
	var sb strings.Builder
	for i := 0; i < api.MaxReadLines+10; i++ {
		fmt.Fprintf(&sb, "line-%d\n", i)
	}
	os.WriteFile(path, []byte(sb.String()), 0o644)
	lines := api.ReadLines(path)
	if len(lines) != api.MaxReadLines || lines[len(lines)-1] != fmt.Sprintf("line-%d", api.MaxReadLines+9) {
		t.Fatalf("环形缓冲结果不正确: len=%d last=%s", len(lines), lines[len(lines)-1])
	}
	// 行数不超过窗口：原样返回
	os.WriteFile(path, []byte("a\nb\n"), 0o644)
	if got := api.ReadLines(path); len(got) != 2 || got[0] != "a" {
		t.Fatalf("短文件 = %v", got)
	}
}

func TestAPIEventsSanitizePrompt(t *testing.T) {
	if got := api.SanitizePrompt(`a"b\c` + "\n"); got != "abc" {
		t.Fatalf("sanitizePrompt = %q", got)
	}
	long := strings.Repeat("x", 100)
	if got := api.SanitizePrompt(long); len([]rune(got)) != 60 {
		t.Fatalf("应截断到 60 字符: %d", len(got))
	}
}
