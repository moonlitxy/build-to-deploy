package k8s

import "time"

// FormatDurationForTest 包装 formatDuration（Pod 存活时长展示）
func FormatDurationForTest(d time.Duration) string { return formatDuration(d) }
