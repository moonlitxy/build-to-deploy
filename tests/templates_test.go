package tests

import (
	"build-to-deploy/internal/templates"
	"strings"
	"testing"

	"build-to-deploy/internal/config"
)

func TestTemplatesArtifactName(t *testing.T) {
	if templates.ArtifactName("jar") != "app.jar" || templates.ArtifactName("go") != "app" || templates.ArtifactName("binary") != "app" {
		t.Fatal("产物名不正确")
	}
}

func TestTemplatesRender(t *testing.T) {
	a := &config.AppConfig{Name: "demo", Type: "jar"}
	jar, err := templates.Render(a, "v1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`build-to-deploy.app="demo"`, `build-to-deploy.version="v1.0.0"`, "COPY app.jar /app/app.jar", `ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]`} {
		if !strings.Contains(jar, want) {
			t.Fatalf("jar 模板缺少 %q:\n%s", want, jar)
		}
	}
	gobin, err := templates.Render(&config.AppConfig{Name: "demo", Type: "go"}, "v2")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gobin, `ENTRYPOINT ["/app/app"]`) || !strings.Contains(gobin, "RUN chmod +x /app/app") {
		t.Fatalf("go 模板不正确:\n%s", gobin)
	}
	if _, err := templates.Render(&config.AppConfig{Name: "demo", Type: "unknown"}, "v1"); err == nil {
		t.Fatal("未知类型应报错")
	}
}
