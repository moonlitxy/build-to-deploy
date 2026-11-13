package dockerops

// 集中测试（tests/）使用的导出入口：包装包内解析函数，生产代码不调用。

// UsedTagsForTest 包装 parseUsedTags
func UsedTagsForTest(out, imageRepo string) map[string]bool { return parseUsedTags(out, imageRepo) }
