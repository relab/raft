.PHONY: proto
proto:
	protoc --gorums_out=plugins=grpc+gorums:. proto/gorums/raft.proto
	cd proto/gorums; go install .
