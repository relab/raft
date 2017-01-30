.PHONY: all
all: test

.PHONY: autocomplete
autocomplete:
	go install .

.PHONY: protocgorums
protocgorums:
	go install github.com/relab/gorums/cmd/protoc-gen-gorums

.PHONY: proto
	protoc -I ../../../:. --gorums_out=plugins=grpc+gorums:. gorumspb/gorums.proto
	protoc -I ../../../:. --gogofast_out=. raftpb/raft.proto
proto: protocgorums

.PHONY: test
test:
	go test -v

.PHONY: bench
bench:
	go test -v -run ^none -bench .

.PHONY: lint
check:
	@gometalinter --config metalinter.json
