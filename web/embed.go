// Package web 内嵌前端静态资源，编译后单个二进制即可运行。
package web

import "embed"

//go:embed static
var FS embed.FS
