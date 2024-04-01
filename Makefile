.PHONY: test build coverage

test:
	go test -race -p 1 -count=1 -tags=integration -covermode=atomic -coverprofile=coverage.cov ./...

build:
	go build -o bin/lock test-examples/lock/main.go

coverage:
	go tool cover -func coverage.cov | grep ^total
