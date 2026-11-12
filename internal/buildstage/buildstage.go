package buildstage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Spec 从构建命令中解析出的构建要素
type Spec struct {
	Dir        string   // 构建上下文目录（docker build 末尾的参数）
	Dockerfile string   // Dockerfile 的绝对路径（-f 或构建目录下的 Dockerfile）
	Tagged     bool     // 命令里是否带上 {{.Tag}}：不带时新 Tag 构建出的镜像内容必然相同
	Inputs     []string // 参与指纹的构建上下文路径（绝对路径）
	Skipped    int      // 无法静态解析、已跳过计算的输入数量
	baseDir    string   // 构建目录解析后的真实位置，指纹里用它把输入路径换算成相对路径
}

var (
	tagPlaceholderRe = regexp.MustCompile(`\{\{\s*\.Tag\s*\}\}`)
	tokenSplitRe     = regexp.MustCompile(`'[^']*'|"[^"]*"|\S+`)
	copyFromRe       = regexp.MustCompile(`(?i)^--from=`)
)

// Parse 从渲染后的构建命令（单行 sh，或换行分隔的多行脚本）中解析构建要素。
// 只做静态分析：无法识别的写法返回 nil，由调用方跳过指纹检测，不影响命令执行。
func Parse(cmd string) *Spec {
	line := lastBuildLine(cmd)
	if line == "" {
		return nil
	}
	toks, ok := shellSplit(line)
	if !ok {
		return nil
	}
	start := -1
	for i := 0; i+1 < len(toks); i++ {
		if toks[i] == "docker" {
			if toks[i+1] == "build" {
				start = i + 2
				break
			}
			if toks[i+1] == "buildx" && i+3 < len(toks) && toks[i+2] == "build" {
				start = i + 3
				break
			}
		}
	}
	if start < 0 {
		return nil
	}

	spec := &Spec{}
	args := toks[start:]
	// 上下文为 "." 时按 docker 规则指向上一次 cd 的目录（本机 shell 与远程 sh 一致）
	cwdHint := lastCdTarget(toks[:start])
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-f" || a == "--file":
			if i+1 < len(args) {
				spec.Dockerfile = args[i+1]
				i++
			}
		case strings.HasPrefix(a, "--file="):
			spec.Dockerfile = strings.TrimPrefix(a, "--file=")
		case a == "--build-arg" || a == "--target" || a == "--platform" || a == "--progress" ||
			a == "--network" || a == "--cache-from" || a == "--cache-to" || a == "--secret" ||
			a == "--ssh" || a == "--label" || a == "--add-host" || a == "--iidfile" ||
			a == "--output" || a == "-o" || a == "--shm-size" || a == "--memory" ||
			a == "--cpu-shares" || a == "-t" || a == "--tag":
			// 带值的构建参数整体跳过；build-arg 的值可能改变产物，计入"无法解析"
			if a == "--build-arg" {
				spec.Skipped++
			}
			i++
		case strings.HasPrefix(a, "--build-arg="):
			spec.Skipped++
		case strings.HasPrefix(a, "-"):
			// 其余开关（如 --no-cache、--pull）不影响输入文件集合
		default:
			if spec.Dir == "" {
				spec.Dir = strings.TrimRight(a, "/")
				if spec.Dir == "." && cwdHint != "" {
					spec.Dir = cwdHint
				}
			}
		}
	}
	if spec.Dir == "" {
		return nil
	}
	var cwd string
	if !strings.HasPrefix(spec.Dir, "/") {
		cwd = cwdHint
		if cwd == "" {
			return nil
		}
	}
	join := func(base, p string) string {
		if p == "" || strings.HasPrefix(p, "/") {
			return p
		}
		if base == "" || base == "." {
			if base == "." {
				return "./" + p
			}
			return p
		}
		return strings.TrimRight(base, "/") + "/" + p
	}

	dir := spec.Dir
	if dir != "." {
		dir = join(cwd, dir)
	} else {
		dir = cwd
		if dir == "" {
			dir = "."
		}
	}
	dockerfile := spec.Dockerfile
	if dockerfile == "" {
		dockerfile = "Dockerfile"
	}
	dockerfile = join(dir, dockerfile)

	base := strings.TrimSuffix(dir, "/")
	inputs, skipped, ok := dockerfileInputs(dockerfile, base)
	if !ok {
		return nil
	}
	spec.Inputs = inputs
	spec.Skipped += skipped
	spec.baseDir = base
	spec.Tagged = tagPlaceholderRe.MatchString(cmd)
	return spec
}

// lastBuildLine 取最后一条执行 docker build 的行：命令模板可能是多行脚本，只有最后一行真正构建
func lastBuildLine(cmd string) string {
	lines := strings.Split(cmd, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		if hasShellWord(l, "docker") && (hasShellWord(l, "build") || hasShellWord(l, "buildx")) {
			return l
		}
	}
	return ""
}

// hasShellWord 判断命令中是否存在独立单词（容忍前后是引号、空白或控制符）
func hasShellWord(cmd, word string) bool {
	for i := 0; i+len(word) <= len(cmd); i++ {
		if cmd[i:i+len(word)] != word {
			continue
		}
		before := i == 0 || strings.ContainsRune("'\" \t;&|(", rune(cmd[i-1]))
		after := i+len(word) == len(cmd) || strings.ContainsRune("'\" \t;&|)", rune(cmd[i+len(word)]))
		if before && after {
			return true
		}
	}
	return false
}

// shellSplit 按引号规则切分单词，并去掉控制运算符（&&、;、> 等）及其参数。
// 引号会被去掉但单词保持为一个整体；认不出的运算符（如 2>&1）直接放弃解析
func shellSplit(s string) ([]string, bool) {
	var out []string
	skipNext := false
	for i := 0; i < len(s); {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' {
			i++
			continue
		}
		if skipNext {
			// 重定向目标：跳到下一个空白
			for i < len(s) && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' {
				i++
			}
			skipNext = false
			continue
		}
		if i+1 < len(s) && ((s[i] == '&' && s[i+1] == '&') || (s[i] == '|' && s[i+1] == '|')) {
			i += 2
			continue
		}
		if c == ';' || c == '|' || c == '&' {
			i++
			continue
		}
		if (c == '1' || c == '2') && i+1 < len(s) && s[i+1] == '>' {
			// 2>&1 / 2>file：丢掉文件描述符，剩下的交给下面的重定向分支
			i++
			c = s[i]
		}
		if c == '>' || c == '<' {
			for i < len(s) && (s[i] == '>' || s[i] == '<' || s[i] == '|') {
				i++
			}
			if i < len(s) && s[i] == '&' {
				// &1 之类的目标直接跳过
				i++
				for i < len(s) && s[i] >= '0' && s[i] <= '9' {
					i++
				}
				continue
			}
			if i < len(s) && (s[i] == ' ' || s[i] == '\t') {
				skipNext = true
			}
			continue
		}
		if !(c == '\'' || c == '"' || c == '$' || c == '(' || c == '`' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || strings.ContainsRune("._-/:@=+,~%", rune(c))) {
			return nil, false
		}
		var b strings.Builder
		for i < len(s) {
			c = s[i]
			if c == '\'' {
				j := strings.IndexByte(s[i+1:], '\'')
				if j < 0 {
					return nil, false
				}
				b.WriteString(s[i+1 : i+1+j])
				i += j + 2
				continue
			}
			if c == '"' {
				j := strings.IndexByte(s[i+1:], '"')
				if j < 0 {
					return nil, false
				}
				b.WriteString(s[i+1 : i+1+j])
				i += j + 2
				continue
			}
			if c == ' ' || c == '\t' || c == '\n' || c == '&' || c == '|' || c == ';' || c == '>' || c == '<' || c == '(' || c == ')' {
				break
			}
			b.WriteByte(c)
			i++
		}
		out = append(out, b.String())
	}
	return out, true
}

type dockerInstr struct {
	kind    string // stage（FROM 产生的阶段边界）、copy、add、other
	flags   []string
	args    []string
	unknown bool // JSON 数组写法等无法静态解析的 COPY
}

// dockerfileInputs 解析 Dockerfile 中 COPY/ADD 的来源，返回绝对路径。
// 通配符、变量、ADD URL 等无法展开的来源不计入路径，但会让指纹带上"不完整"标记。
func dockerfileInputs(path, dir string) (inputsOut []string, skipped int, ok bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, false
	}
	seen := map[string]bool{}
	var inputs []string
	add := func(p string) {
		if p == "" || seen[p] {
			return
		}
		seen[p] = true
		inputs = append(inputs, p)
	}
	add(path)

	markSkipped := func() {
		skipped++
		// 第 n 个无法解析的来源用不同占位符，保证"在 Dockerfile 里新增一个同类 COPY"也会改变指纹
		add(fmt.Sprintf("!%d", skipped))
	}

	for _, ins := range parseDockerfile(string(data)) {
		if ins.kind != "copy" && ins.kind != "add" {
			continue
		}
		fromStage := false
		for _, f := range ins.flags {
			if copyFromRe.MatchString(f) {
				fromStage = true
			}
		}
		if ins.unknown || len(ins.args) < 2 {
			// 无法解析：用占位符保证指纹对 Dockerfile 原文变化保持敏感
			markSkipped()
			continue
		}
		if fromStage {
			markSkipped()
			continue
		}
		for _, src := range ins.args[:len(ins.args)-1] {
			if strings.Contains(src, "://") {
				markSkipped()
				continue
			}
			p := strings.TrimPrefix(src, "./")
			if p == "" {
				continue
			}
			if strings.HasPrefix(p, "/") {
				// 绝对路径位于构建上下文之外，不参与指纹
				markSkipped()
				continue
			}
			add(dir + "/" + p)
			if strings.ContainsAny(p, "*?[") {
				markSkipped()
			}
		}
	}
	return inputs, skipped, true
}

// parseDockerfile 逐条解析指令，处理续行符与注释
func parseDockerfile(content string) []dockerInstr {
	var joined []string
	var cur strings.Builder
	for _, l := range strings.Split(content, "\n") {
		t := strings.TrimSpace(l)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		if strings.HasSuffix(t, "\\") {
			cur.WriteString(strings.TrimSuffix(t, "\\"))
			cur.WriteString(" ")
			continue
		}
		cur.WriteString(t)
		joined = append(joined, cur.String())
		cur.Reset()
	}
	if cur.Len() > 0 {
		joined = append(joined, cur.String())
	}

	out := make([]dockerInstr, 0, len(joined))
	for _, line := range joined {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		keyword := strings.ToUpper(fields[0])
		if keyword == "FROM" {
			out = append(out, dockerInstr{kind: "stage"})
			continue
		}
		if keyword != "COPY" && keyword != "ADD" {
			out = append(out, dockerInstr{kind: "other"})
			continue
		}
		ins := dockerInstr{kind: strings.ToLower(keyword)}
		rest := strings.TrimSpace(line[len(fields[0]):])
		for strings.HasPrefix(rest, "--") {
			sp := strings.IndexAny(rest, " \t")
			if sp < 0 {
				ins.flags = append(ins.flags, rest)
				rest = ""
				break
			}
			ins.flags = append(ins.flags, rest[:sp])
			rest = strings.TrimSpace(rest[sp+1:])
		}
		if strings.HasPrefix(rest, "[") {
			ins.unknown = true
			out = append(out, ins)
			continue
		}
		for _, a := range tokenSplitRe.FindAllString(rest, -1) {
			ins.args = append(ins.args, strings.Trim(a, `"'`))
		}
		out = append(out, ins)
	}
	return out
}

// ctxDir 返回构建上下文目录（相对 "." 时已换算为真实位置）
func (s *Spec) ctxDir() string {
	return s.baseDir
}

// BaseDir 返回构建上下文目录的绝对路径；无法确定时返回空串
func (s *Spec) BaseDir() string {
	if strings.HasPrefix(s.baseDir, "/") {
		return s.baseDir
	}
	return ""
}

// expandInputs 把通配符来源展开成实际文件列表；目录来源按目录下全部普通文件计算
func expandInputs(dir string, paths []string) ([]string, bool) {
	var out []string
	for _, p := range paths {
		if strings.HasPrefix(p, "!") {
			continue
		}
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		if !info.IsDir() {
			if strings.ContainsAny(p, "*?[") {
				matches, _ := filepath.Glob(p)
				if len(matches) == 0 {
					// 通配符无匹配：构建时同样会失败，按缺失处理
					out = append(out, p)
				}
				out = append(out, matches...)
				continue
			}
			out = append(out, p)
			continue
		}
		err = filepath.Walk(p, func(path string, fi os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if fi.Mode().IsRegular() {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			return nil, false
		}
	}
	return out, true
}

// Fingerprint 计算指纹：产物内容摘要 + 构建上下文文件的 SHA256。
// skipped 表示有多少输入未能静态解析（指纹可能不完整），调用方据此在日志里提示。
func (s *Spec) Fingerprint(artifactSHA string) (fp string, skipped int) {
	h := sha256.New()
	fmt.Fprintf(h, "artifact:%s\n", artifactSHA)

	inputs := append([]string(nil), s.Inputs...)
	if d := s.ctxDir(); d != "" {
		more, ok := expandInputs(d, s.Inputs)
		if ok {
			inputs = append(inputs, more...)
		}
	}
	sort.Strings(inputs)
	skipped = s.Skipped
	for _, p := range inputs {
		if strings.HasPrefix(p, "!") {
			// 无法静态解析的来源已在解析时计入 Skipped，这里只让它们参与摘要
			fmt.Fprintf(h, "unparsed-input:%s\n", p)
			continue
		}
		// 路径去掉构建目录前缀再入摘要：同一套配置换一个构建目录，指纹不应变化
		rel := "~/" + p
		if s.baseDir != "" && strings.HasPrefix(p, s.baseDir+"/") {
			rel = strings.TrimPrefix(p, s.baseDir+"/")
		} else if strings.HasPrefix(p, "./") && s.baseDir != "" && s.baseDir != "." {
			// 构建目录是绝对路径时，COPY 来源按绝对路径记录，这里统一换算成相对
			rel = strings.TrimPrefix(p, "./")
		}
		sum, err := fileSHA256(p)
		if err != nil {
			// 不存在的来源按"缺失"计入：构建阶段本身会报错，这里只需保持指纹稳定
			fmt.Fprintf(h, "%s:MISSING\n", rel)
			continue
		}
		fmt.Fprintf(h, "%s:%s\n", rel, sum)
	}
	return hex.EncodeToString(h.Sum(nil)), skipped
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// SummarizeImages 从镜像 HEAD 命令的输出（format 已把 ID 与 RepoDigests 拼在一行）里提取镜像摘要。
// format 形如 "{{.Id}} {{json .RepoDigests}}"，兼容本地 ID（sha256:短）与完整摘要 sha256:<64>hex 两种写法。
func SummarizeImages(output, repo, tag string) []string {
	var out []string
	seen := map[string]bool{}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		for i := 0; i < len(line); i++ {
			if !strings.HasPrefix(line[i:], "sha256:") {
				continue
			}
			j := i + len("sha256:")
			for j < len(line) && (line[j] >= '0' && line[j] <= '9' || line[j] >= 'a' && line[j] <= 'f') {
				j++
			}
			id := line[i:j]
			i = j - 1
			if len(id) != len("sha256:")+64 && len(id) != len("sha256:")+12 {
				continue
			}
			if seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// Short 返回便于日志展示的摘要前缀
func Short(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// lastCdTarget 取命令片段里最后一次 cd 的目标目录（形如 "cd /a/b"），用于推算 "." 上下文的真实位置
func lastCdTarget(toks []string) string {
	target := ""
	for i := 0; i+1 < len(toks); i++ {
		if toks[i] == "cd" {
			target = toks[i+1]
		}
	}
	return target
}
