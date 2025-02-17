GOTOOLS = github.com/golangci/golangci-lint/cmd/golangci-lint
PACKAGES=$(shell go list ./...)
INCLUDE = -I=${GOPATH}/src/github.com/tendermint/tm-db -I=${GOPATH}/src -I=${GOPATH}/src/github.com/gogo/protobuf/protobuf

export GO111MODULE = on

all: lint test

### go tests
## By default this will only test memdb & goleveldb
test:
	@echo "--> Running go test"
	@go test $(PACKAGES) -v

test-memdb:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags memdb -v

test-goleveldb:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags goleveldb -v

test-cleveldb:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags cleveldb -v

test-rocksdb:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags rocksdb -v

test-boltdb:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags boltdb -v

test-badgerdb:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags badgerdb -v

test-mongodb:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags mongodb,goleveldb -v  -count=1

bench-mongodb:
	@echo "--> Running mongodb bench"
	@go  test -benchmem -run=^$ -tags=tests,mongodb,goleveldb  -bench ^BenchmarkMongoDBBsonReadsWrites$  github.com/tendermint/tm-db/mongodb  -count=1 -p 1

bench-goleveldb:
	@echo "--> Running goleveldb bench"
	@go  test -benchmem -run=^$ -tags=tests,goleveldb  -bench ^Benchmark.*$  github.com/tendermint/tm-db/goleveldb  -count=1 -p 1

test-all:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags memdb,goleveldb,cleveldb,boltdb,rocksdb,badgerdb,mongodb -v

test-all-docker:
	@echo "--> Running go test"
	@docker run --rm -v $(CURDIR):/workspace --workdir /workspace tendermintdev/docker-tm-db-testing go test $(PACKAGES) -tags memdb,goleveldb,cleveldb,boltdb,rocksdb,badgerdb -v
.PHONY: test-all-docker

lint:
	@echo "--> Running linter"
	# @golangci-lint run
	@go mod verify
.PHONY: lint

format:
	find . -name '*.go' -type f -not -path "*.git*" -not -name '*.pb.go' -not -name '*pb_test.go' | xargs gofmt -w -s
	find . -name '*.go' -type f -not -path "*.git*"  -not -name '*.pb.go' -not -name '*pb_test.go' | xargs goimports -w
.PHONY: format

tools:
	go get -v $(GOTOOLS)

# generates certificates for TLS testing in remotedb
gen_certs: clean_certs
	certstrap init --common-name "tendermint.com" --passphrase ""
	certstrap request-cert --common-name "remotedb" -ip "127.0.0.1" --passphrase ""
	certstrap sign "remotedb" --CA "tendermint.com" --passphrase ""
	mv out/remotedb.crt remotedb/test.crt
	mv out/remotedb.key remotedb/test.key
	rm -rf out

clean_certs:
	rm -f db/remotedb/test.crt
	rm -f db/remotedb/test.key

%.pb.go: %.proto
	## If you get the following error,
	## "error while loading shared libraries: libprotobuf.so.14: cannot open shared object file: No such file or directory"
	## See https://stackoverflow.com/a/25518702
	## Note the $< here is substituted for the %.proto
	## Note the $@ here is substituted for the %.pb.go
	protoc $(INCLUDE) $< --gogo_out=Mgoogle/protobuf/timestamp.proto=github.com/golang/protobuf/ptypes/timestamp,plugins=grpc:../../..


protoc_remotedb: remotedb/proto/defs.pb.go	
