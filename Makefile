.PHONY: build
build:
	gvt restore
	protoc --gorums_out=plugins=grpc+gorums:. proto/gorums/raft.proto
	cd proto/gorums; go install .
