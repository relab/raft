.PHONY: autocomplete
autocomplete:
	go install .
	cd raftpb; go install .

.PHONY: protocgorums
protocgorums:
	go install github.com/relab/gorums/cmd/protoc-gen-gorums

.PHONY: proto
proto: protocgorums
	protoc -I ../../../:. --gorums_out=plugins=grpc+gorums:. raftpb/raft.proto

.PHONY: restore
restore:
	gvt restore
