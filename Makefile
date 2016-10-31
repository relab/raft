.PHONY: build
build:
	cd proto/gorums; go install .
	cd debug; go install .

.PHONY: protocgorums
protocgorums:
	go install github.com/relab/gorums/cmd/protoc-gen-gorums

.PHONY: proto
proto: protocgorums
	protoc -I ../../../:. --gorums_out=plugins=grpc+gorums:. proto/gorums/raft.proto

.PHONY: restore
restore:
	gvt restore
