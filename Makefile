##########################################################################
# Configuration:

# PROTOC_INC_GO_MODULES are Go modules which must be resolved and included
# with `protoc` invocations
PROTOC_INC_GO_MODULES = \
	github.com/golang/protobuf \
	github.com/gogo/protobuf \
	go.gazette.dev/core

# Targets of Go protobufs which must be compiled.
GO_PROTO_TARGETS = \
	./capture/capture.pb.go \
	./flow/flow.pb.go \
	./materialize/materialize.pb.go

# GO_MODULE_PATH expands a $(module), like "go.gazette.dev/core", to the local path
# of its respository as currently specified by go.mod. The `go list` tool
# is used to map submodules to corresponding go.mod versions and paths.
GO_MODULE_PATH = $(shell go list -f '{{ .Dir }}' -m $(module))

##########################################################################
# Build rules:

.PHONY: print-versions
print-versions:
	echo "Resolved repository version: ${VERSION}" \
		&& go version \
		&& protoc --version

.PHONY: protoc-gen-gogo
protoc-gen-gogo:
	go mod download
	go install github.com/gogo/protobuf/protoc-gen-gogo

# Run the protobuf compiler to generate message and gRPC service implementations.
# Invoke protoc with local and third-party include paths set.
# The M_ option in gogo_out is some weird crap that allows using the imported struct type with gogo.
# I have no idea how or why it works, but I took it from this GH comment: 
# https://github.com/gogo/protobuf/issues/325#issuecomment-335144716
%.pb.go: %.proto protoc-gen-gogo
	PATH=$$PATH:$(shell go env GOPATH)/bin \
	protoc -I . $(foreach module, $(PROTOC_INC_GO_MODULES), -I$(GO_MODULE_PATH)) \
		--gogo_out=paths=source_relative,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,plugins=grpc:. $*.proto

go-protobufs: $(GO_PROTO_TARGETS)

.PHONY: go-test
go-test: $(GO_PROTO_TARGETS)
	go test -v ./...
