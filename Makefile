VERSION ?= $(shell date +%Y%m%d%H%M)
LDFLAGS := -s -w -X main.version=$(VERSION)
MAIN := .

.PHONY: build linux windows darwin dev

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/build-to-deploy $(MAIN)

linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/build-to-deploy-linux-amd64 $(MAIN)

windows:
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/build-to-deploy.exe $(MAIN)

darwin:
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/build-to-deploy-darwin-arm64 $(MAIN)

dev:
	@if [ ! -f configs/config.dev.yaml ]; then \
		sed -e 's|^  dataDir: "./data".*|  dataDir: "./data-dev"|' \
		    -e 's|^  dryRun: false.*|  dryRun: true             # 演练模式：不连接远程主机，仅模拟构建与部署|' \
		    configs/config.example.yaml > configs/config.dev.yaml; \
		echo "已根据 configs/config.example.yaml 生成演练配置: configs/config.dev.yaml"; \
	fi
	go run $(MAIN) -config configs/config.dev.yaml
