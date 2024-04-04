.PHONY: test lint build coverage

test:
	go test -race -p 1 -count=1 -tags=integration -covermode=atomic -coverprofile=coverage.out ./...

lint:
	$(foreach f,$(shell go fmt ./...),@echo "Forgot to format file: ${f}"; exit 1;)
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

build:
	go build -o bin/lock test-examples/lock/main.go

coverage:
	go tool cover -func coverage.out | grep ^total
