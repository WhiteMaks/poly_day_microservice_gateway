up-test-environment:
	docker-compose up -d
	sleep 60

test:
	go clean -testcache && go test -cover -v ./...

build:
	go build -o ./.bin/gateway/poly_day_microservice_gateway cmd/main.go

setup-git:
	git config --global url."ssh://git@github.com".insteadOf "https://github.com"

.PHONY: build test up-test-environment setup-git