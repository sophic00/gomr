.PHONY: all build clean test run-master run-worker fmt vet

# Binary name
APP_NAME := gomr

# Build directory
BIN_DIR := bin

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

# Run the master node locally
run-master: build
	@echo "Starting Master node..."
	./$(BIN_DIR)/$(APP_NAME) master -port 8080

# Run a worker node locally
run-worker: build
	@echo "Starting Worker node..."
	./$(BIN_DIR)/$(APP_NAME) worker -master localhost:8080 -port 8081

# Plugin configuration
PLUGIN_NAME ?= wordcount
PLUGIN_PATH ?= ./plugin/$(PLUGIN_NAME)

# Build the plugin
build-plugin:
	@echo "Building $(PLUGIN_NAME) plugin from $(PLUGIN_PATH)..."
	@mkdir -p $(BIN_DIR)/plugins
	go build -buildmode=plugin -o $(BIN_DIR)/plugins/$(PLUGIN_NAME).so $(PLUGIN_PATH)

# Upload plugin to S3
upload-plugin: build-plugin
	@echo "Uploading $(PLUGIN_NAME) plugin to S3..."
	aws --endpoint-url http://thia:3900 s3 cp $(BIN_DIR)/plugins/$(PLUGIN_NAME).so s3://thia/plugins/$(PLUGIN_NAME).so
