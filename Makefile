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
	go run $(MAIN) -config configs/config.dev.yaml
