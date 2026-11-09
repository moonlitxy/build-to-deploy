// Package templates 内置各类型应用的 Dockerfile 模板（Go text/template 语法）。
//
// 可用变量：.App .Tag .BaseImage .Port .JavaOpts
package templates

import (
	"embed"
	"fmt"
	"strings"
	"text/template"

	"build-to-deploy/internal/config"
)

//go:embed *.Dockerfile
var fs embed.FS

type Data struct {
	App       string
	Tag       string
	BaseImage string
	Port      int
	JavaOpts  string
}

// ArtifactName 产物在构建上下文中的文件名。
func ArtifactName(appType string) string {
	if appType == "jar" {
		return "app.jar"
	}
	return "app"
}

// Render 渲染 Dockerfile。应用配置了自定义模板文件时优先使用。
func Render(app *config.AppConfig, tag string) (string, error) {
	var raw []byte
	var err error
	raw, err = fs.ReadFile(app.Type + ".Dockerfile")
	if err != nil {
		return "", fmt.Errorf("读取 Dockerfile 模板失败: %w", err)
	}
	t, err := template.New("Dockerfile").Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return "", fmt.Errorf("解析 Dockerfile 模板失败: %w", err)
	}
	var sb strings.Builder
	err = t.Execute(&sb, Data{App: app.Name, Tag: tag, BaseImage: "eclipse-temurin:17-jre", Port: 8080, JavaOpts: ""})
	return sb.String(), err
}
