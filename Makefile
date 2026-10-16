GO          ?= go
STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.2
GOVULNCHECK := golang.org/x/vuln/cmd/govulncheck@v1.9.0
GOLANGCI    := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0
GREMLINS    := github.com/go-gremlins/gremlins/cmd/gremlins@v0.5.1

# colima では testcontainers が Docker の場所を見つけられないので、ソケットを明示する。
COLIMA_SOCK := $(HOME)/.colima/default/docker.sock
ifneq ($(wildcard $(COLIMA_SOCK)),)
export DOCKER_HOST ?= unix://$(COLIMA_SOCK)
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE ?= /var/run/docker.sock
endif

.PHONY: help quality ci fmt-check vet staticcheck lint vuln test cover crap mutation e2e up down

help: ## ターゲットの一覧を表示する
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "%-12s %s\n", $$1, $$2}'

quality: ci mutation e2e ## すべての品質ゲートを通す

ci: fmt-check vet staticcheck lint vuln test cover crap ## CI と同じ静的検査とテストを通す

fmt-check: ## gofmt の差分が無いことを確かめる
	@test -z "$$(gofmt -l .)" || (echo "gofmt の差分:"; gofmt -l .; exit 1)

vet: ## go vet
	$(GO) vet ./...

staticcheck: ## staticcheck
	$(GO) run $(STATICCHECK) ./...

lint: ## golangci-lint
	$(GO) run $(GOLANGCI) run ./...

vuln: ## govulncheck（testcontainers などテストだけの依存も含める）
	$(GO) run $(GOVULNCHECK) -test ./...

test: ## 実コンテナを使う結合テストを含めて、race と shuffle 付きで実行する
	$(GO) test -race -shuffle=on -count=1 -coverprofile=coverage.out ./...

cover: ## 全体のカバレッジが80%以上であることを確かめる
	@$(GO) tool cover -func=coverage.out | awk '/^total:/ { v = $$3; sub(/%/, "", v); print "カバレッジ " $$3; if (v + 0 < 80) exit 1 }'

crap: ## すべての関数の CRAP が15未満であることを確かめる
	scripts/crap.sh coverage.out 15

mutation: ## gremlins で、すべてのミュータントがテストに捕まる（KILLED）ことを確かめる
	GREMLINS="$(GO) run $(GREMLINS)" scripts/mutation.sh

e2e: ## compose で全体を起動し、HTTP と CLI で流れを確かめる
	scripts/e2e.sh

up: ## compose で全体を起動する
	docker compose up -d --build --wait mysql valkey
	docker compose up -d --build api relay inventory sales

down: ## compose を止めてボリュームも消す
	docker compose --profile replay down -v
