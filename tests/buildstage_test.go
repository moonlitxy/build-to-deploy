package tests

import (
	"build-to-deploy/internal/buildstage"

	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// writeCtx 在临时目录里准备一个构建上下文
func writeCtx(t *testing.T, dockerfile string, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		full := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestBuildStageParseContextDirAndDockerfile(t *testing.T) {
	dir := writeCtx(t, "FROM scratch\nCOPY app.jar /app.jar\n", map[string]string{"app.jar": "jar"})
	cases := []struct {
		name   string
		cmd    string
		dir    string
		tagged bool
	}{
		{"cd + 相对上下文", "cd " + dir + " && docker build -t a:v1 .", dir, false},
		{"绝对上下文", "docker build -t a:v1 " + dir, dir, false},
		{"带 Tag 变量", "cd " + dir + " && docker build -t a:{{.Tag}} .", dir, true},
		{"-f 指定 Dockerfile", "docker build -f " + dir + "/Dockerfile -t a:v1 " + dir, dir, false},
		{"--file= 形式", "docker build --file=" + dir + "/Dockerfile -t a:v1 " + dir, dir, false},
		{"buildx", "docker buildx build -t a:v1 " + dir, dir, false},
		{"多行脚本取最后一行 build", "printf 'FROM x' > /tmp/x && cd " + dir + " && docker build -t a:v1 .", dir, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sp := buildstage.Parse(c.cmd)
			if sp == nil {
				t.Fatalf("Parse(%q) = nil", c.cmd)
			}
			if sp.BaseDir() != c.dir {
				t.Fatalf("BaseDir = %q, want %q", sp.BaseDir(), c.dir)
			}
			if sp.Tagged != c.tagged {
				t.Fatalf("Tagged = %v, want %v", sp.Tagged, c.tagged)
			}
		})
	}
}

func TestBuildStageParseUnsupportedShape(t *testing.T) {
	cases := []string{
		"echo build",
		"",
		"docker build",
		"cd /tmp && docker build -t a:v1",
		"docker build -t a:v1 .",            // 相对的 "." 且没有 cd，无法还原
		"docker build -t a:v1 relative-dir", // 相对构建目录
		"docker build -t a:v1 -f /elsewhere/Dockerfile /ctx",    // Dockerfile 与上下文不同目录
		"docker build -t a:'unterminated .",                     // 引号不闭合
		"cd /ctx && docker build -t a:v1 . 2>&1",                // 重定向写法不支持
		"cd /nonexistent-dir-for-btd && docker build -t a:v1 .", // Dockerfile 读不到
	}
	for _, cmd := range cases {
		if sp := buildstage.Parse(cmd); sp != nil {
			t.Fatalf("Parse(%q) = %+v, want nil", cmd, sp)
		}
	}
}

func TestBuildStageDockerfileInputs(t *testing.T) {
	dir := writeCtx(t, strings.Join([]string{
		"FROM base AS build",
		"COPY --from=other /x /y",      // 来自其他阶段：不计入路径，但计入 skipped
		`COPY ["a.json", "/a"]`,        // JSON 数组写法：无法解析
		"COPY --chown=1:1 src ./dst",   // 普通复制
		"ADD https://example.com/x /x", // URL 来源
		"COPY missing.txt /m",          // 不存在的来源
		"RUN echo hi \\",
		"  && echo there", // 续行不能影响后续指令
		"# 注释行",
		"COPY *.jar /jars/", // 通配符
	}, "\n"), map[string]string{"src/x": "x", "a.jar": "a"})

	sp := buildstage.Parse("cd " + dir + " && docker build -t a:v1 .")
	if sp == nil {
		t.Fatal("Parse = nil")
	}
	fp, skipped := sp.Fingerprint("artifact")
	if fp == "" {
		t.Fatal("指纹为空")
	}
	if skipped < 3 {
		t.Fatalf("skipped = %d，期望至少 3（--from、JSON 写法、URL）", skipped)
	}

	var want []string
	for _, p := range sp.Inputs {
		want = append(want, strings.TrimPrefix(p, dir+"/"))
	}
	joined := strings.Join(want, ",")
	for _, expect := range []string{"Dockerfile", "src", "missing.txt", "*.jar"} {
		if !strings.Contains(joined, expect) {
			t.Fatalf("输入缺少 %s: %v", expect, sp.Inputs)
		}
	}
	for _, unexpected := range []string{"/x", "https"} {
		if strings.Contains(joined, unexpected) {
			t.Fatalf("输入不应包含 %s: %v", unexpected, sp.Inputs)
		}
	}
}

func TestBuildStageFingerprintSemantics(t *testing.T) {
	dir := writeCtx(t, "FROM base\nCOPY app.jar /app.jar\nCOPY conf /etc/conf\n",
		map[string]string{"app.jar": "v1", "conf/a": "1", "unrelated.txt": "x"})
	cmd := "cd " + dir + " && docker build -t a:{{.Tag}} ."

	base, skipped := buildstage.Parse(cmd).Fingerprint("sha-artifact")
	if skipped != 0 {
		t.Fatalf("skipped=%d", skipped)
	}

	// 只换 Tag / 构建命令不改变指纹
	other, _ := buildstage.Parse("cd " + dir + " && docker build -t b:other-tag .").Fingerprint("sha-artifact")
	if other != base {
		t.Fatal("Tag 或镜像名变化不应改变指纹")
	}

	// 产物摘要变化
	if withArtifact, _ := buildstage.Parse(cmd).Fingerprint("sha-artifact-2"); withArtifact == base {
		t.Fatal("产物摘要变化必须改变指纹")
	}

	// 未被 COPY 的文件变化
	if err := os.WriteFile(filepath.Join(dir, "unrelated.txt"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	if same, _ := buildstage.Parse(cmd).Fingerprint("sha-artifact"); same != base {
		t.Fatal("无关文件变化不应改变指纹")
	}

	// COPY 的目录内容变化
	if err := os.WriteFile(filepath.Join(dir, "conf/a"), []byte("2"), 0o644); err != nil {
		t.Fatal(err)
	}
	afterConf, _ := buildstage.Parse(cmd).Fingerprint("sha-artifact")
	if afterConf == base {
		t.Fatal("COPY 目录内容变化必须改变指纹")
	}

	// Dockerfile 变化
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM base\nCOPY app.jar /app.jar\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if afterDf, _ := buildstage.Parse(cmd).Fingerprint("sha-artifact"); afterDf == afterConf {
		t.Fatal("Dockerfile 变化必须改变指纹")
	}

	// 换一个内容相同的构建目录：指纹一致
	dir2 := writeCtx(t, "FROM base\nCOPY app.jar /app.jar\n", map[string]string{"app.jar": "v1"})
	other2, _ := buildstage.Parse("cd " + dir2 + " && docker build -t a:{{.Tag}} .").Fingerprint("sha-artifact")
	if afterDf, _ := buildstage.Parse(cmd).Fingerprint("sha-artifact"); other2 != afterDf {
		t.Fatal("内容相同的不同构建目录应得到相同指纹")
	}
}

func TestBuildStageFingerprintMissingInputStable(t *testing.T) {
	dir := writeCtx(t, "FROM base\nCOPY will-not-exist.txt /x\n", nil)
	fp1, _ := buildstage.Parse("cd " + dir + " && docker build -t a:v .").Fingerprint("a")
	fp2, _ := buildstage.Parse("cd " + dir + " && docker build -t a:v .").Fingerprint("a")
	if fp1 != fp2 {
		t.Fatal("缺失来源的指纹应保持稳定")
	}
}

func TestBuildStageShellSplit(t *testing.T) {
	cases := []struct {
		in   string
		want []string
		ok   bool
	}{
		{"docker build -t 'a b' .", []string{"docker", "build", "-t", "a b", "."}, true},
		{`docker run "x" y`, []string{"docker", "run", "x", "y"}, true},
		{"a && b || c ; d", []string{"a", "b", "c", "d"}, true},
		{"echo hi > out.txt && ls", []string{"echo", "hi", "ls"}, true},
		{"x 2>&1 y", []string{"x", "y"}, true},
		{"x y 2>&1 | tee log", []string{"x", "y", "tee", "log"}, true},
		{"x >", []string{"x"}, true},
		{"'open", nil, false},
		{"", nil, true},
	}
	for _, c := range cases {
		got, ok := buildstage.ShellSplitForTest(c.in)
		if ok != c.ok {
			t.Fatalf("shellSplit(%q) ok=%v want %v", c.in, ok, c.ok)
		}
		if ok && !reflect.DeepEqual(got, c.want) {
			t.Fatalf("shellSplit(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBuildStageLastBuildTarget(t *testing.T) {
	toks := []string{"cd", "/a", "&&", "cd", "/b", "&&", "docker"}
	if got := buildstage.LastCdTargetForTest(toks); got != "/b" {
		t.Fatalf("lastCdTarget = %q", got)
	}
	if got := buildstage.LastCdTargetForTest([]string{"ls"}); got != "" {
		t.Fatalf("无 cd 时应为空: %q", got)
	}
	if !buildstage.HasShellWordForTest("cd /a && docker build", "build") {
		t.Fatal("hasShellWord 应识别独立单词")
	}
	if buildstage.HasShellWordForTest("rebuilder docker", "build") {
		t.Fatal("hasShellWord 不应匹配单词的一部分")
	}
	if got := buildstage.LastBuildLineForTest("# docker build\n\necho x\ndocker build -t a:v .\n"); got != "docker build -t a:v ." {
		t.Fatalf("lastBuildLine = %q", got)
	}
	if got := buildstage.LastBuildLineForTest("# 只有注释"); got != "" {
		t.Fatalf("lastBuildLine 应为空: %q", got)
	}
}

func TestBuildStageSummarizeImagesAndShort(t *testing.T) {
	digest := "sha256:" + strings.Repeat("ab", 32)
	out := "sha256:1234567890ab []\n" + digest + " [\"" + digest + "\"]\n" + digest + " 重复\n"
	got := buildstage.SummarizeImages(out, "repo/x", "v1")
	if len(got) != 2 || got[0] != "sha256:1234567890ab" || got[1] != digest {
		t.Fatalf("SummarizeImages = %v", got)
	}
	if got := buildstage.SummarizeImages("\n  \n", "", ""); len(got) != 0 {
		t.Fatalf("空输出应为空: %v", got)
	}
	if buildstage.Short("abcdef", 3) != "abc" || buildstage.Short("ab", 5) != "ab" {
		t.Fatal("Short 截断不正确")
	}
	if _, err := buildstage.FileSHA256ForTest(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("读取不存在的文件应报错")
	}
}
