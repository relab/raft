PKGS		 := $(shell go list ./... | grep -ve "vendor")
CMD_PKGS := $(shell go list ./... | grep -ve "vendor" | grep "cmd")

.PHONY: all
all: install test

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
	protoc -I ../../../:. --gorums_out=plugins=grpc+gorums:. pkg/raft/raftpb/raft.proto

.PHONY: install
install: proto
	@for pkg in $(CMD_PKGS); do \
		! go install $$pkg; \
		echo $$pkg; \
	done

.PHONY: test
test: proto
	go test $(PKGS) -v

.PHONY: clean
clean:
	go clean -i $(CMD_PKGS)
