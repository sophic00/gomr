.PHONY: all build clean test run-master run-worker fmt vet proto

# Binary name
APP_NAME := gomr

# Build directory
BIN_DIR := bin

PROTO_FILES := $(shell find proto -name '*.proto' -print)

# Default target
all: build

# Build the project
build:
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(APP_NAME) ./cmd/gomr

# Clean build artifacts
clean:
	@echo "Cleaning up..."
	@rm -rf $(BIN_DIR)

# Run tests
test:
	@echo "Running tests..."
	go test -v -race ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Run go vet
vet:
	@echo "Vetting code..."
	go vet ./...

# Generate protobuf Go bindings
proto:
	@echo "Generating protobuf files..."
	PATH="$(shell go env GOPATH)/bin:$$PATH" protoc \
		--proto_path=. \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

# Run the master node locally
run-master: build
	@echo "Starting Master node..."
	./$(BIN_DIR)/$(APP_NAME) master

# Run a worker node locally
run-worker: build
	@echo "Starting Worker node..."
	./$(BIN_DIR)/$(APP_NAME) worker

# Upload example data to S3
upload-data:
	@echo "Uploading example data to S3..."
	aws --endpoint-url http://thia:3900 s3 sync examples/wordcount/data/ s3://thia/data/wordcount/

# Clean data and output from S3
clean-s3:
	@echo "Cleaning S3..."
	aws --endpoint-url http://thia:3900 s3 rm s3://thia/data/ --recursive || true
	aws --endpoint-url http://thia:3900 s3 rm s3://thia/output/ --recursive || true

# List files in S3
ls:
	@echo "Listing S3 contents..."
	@if [ -z "$(DIR)" ]; then \
		aws --endpoint-url http://thia:3900 s3 ls s3://thia/ ; \
	else \
		aws --endpoint-url http://thia:3900 s3 ls s3://thia/$(DIR)/ ; \
	fi
