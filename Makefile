CMD_PKGS := $(shell go list ./... | grep -ve "vendor" | grep "cmd")

.PHONY: all
all: build test

.PHONY: autocomplete
autocomplete:
	go install .
	cd raftpb; go install .

.PHONY: restore
restore:
	gvt restore

.PHONY: protocgorums
protocgorums:
	go install github.com/relab/gorums/cmd/protoc-gen-gorums

.PHONY: proto
proto: protocgorums
	protoc -I ../../../:. --gorums_out=plugins=grpc+gorums:. raftpb/raft.proto

.PHONY: build
build: proto
	@for pkg in $(CMD_PKGS); do \
		! go build $$pkg; \
		echo $$pkg; \
	done

.PHONY: test
test: proto
	go test -v

.PHONY: clean
clean:
	@rm -f replica throughput latency *.storage
