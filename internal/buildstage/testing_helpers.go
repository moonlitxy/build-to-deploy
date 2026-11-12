package buildstage

// 集中测试（tests/）使用的导出入口：包装包内词法分析函数，生产代码不调用。

// ShellSplitForTest 包装 shellSplit
func ShellSplitForTest(s string) ([]string, bool) { return shellSplit(s) }

// LastBuildLineForTest 包装 lastBuildLine
func LastBuildLineForTest(cmd string) string { return lastBuildLine(cmd) }

// LastCdTargetForTest 包装 lastCdTarget
func LastCdTargetForTest(toks []string) string { return lastCdTarget(toks) }

// FileSHA256ForTest 包装 fileSHA256
func FileSHA256ForTest(path string) (string, error) { return fileSHA256(path) }

// HasShellWordForTest 包装 hasShellWord
func HasShellWordForTest(cmd, word string) bool { return hasShellWord(cmd, word) }
