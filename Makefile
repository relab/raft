.PHONY: build
build:
	protoc -I ../../../:. --gorums_out=plugins=grpc+gorums:. proto/gorums/raft.proto
	cd proto/gorums; go install .
	cd debug; go install .

.PHONY: restore
restore:
	gvt restore
