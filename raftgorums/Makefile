.PHONY: all
all: test

.PHONY: autocomplete
autocomplete:
	go install .

.PHONY: protoc
protoc:
	go get github.com/relab/gorums/cmd/protoc-gen-gorums
	go get github.com/gogo/protobuf/protoc-gen-gogofaster

.PHONY: proto
proto: protoc
	protoc -I ../../../../:. --gorums_out=plugins=grpc+gorums:. gorumspb/gorums.proto
	protoc -I ../../../../:. --gogofaster_out=. raftpb/raft.proto

.PHONY: test
test:
	go test -v

.PHONY: bench
bench:
	go test -v -run ^none -bench .

.PHONY: check
check:
	@gometalinter --config ../metalinter.json
