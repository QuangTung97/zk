.PHONY: test

test:
	go test -race -p 1 -count=1 -tags=integration ./...
