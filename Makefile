.PHONY: all
all: test

.PHONY: autocomplete
autocomplete:
	go install .

.PHONY: proto
proto:
	protoc -I ../../../:. --gogofast_out=. raftpb/raft.proto

.PHONY: test
test:
	go test -v

.PHONY: bench
bench:
	go test -v -run ^none -bench .

.PHONY: check
check:
	@gometalinter --config metalinter.json
