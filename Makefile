.PHONY: build
build:
	cd proto/gorums; go install .
	cd debug; go install .

.PHONY: proto
proto:
	protoc -I ../../../:. --gorums_out=plugins=grpc+gorums:. proto/gorums/raft.proto

.PHONY: restore
restore:
	gvt restore
