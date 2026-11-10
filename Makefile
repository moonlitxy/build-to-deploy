VERSION ?= $(shell date +%Y%m%d%H%M)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build linux dev

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/build-to-deploy .

linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/build-to-deploy-linux-amd64 .

dev:
	go run .
