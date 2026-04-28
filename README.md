# Gomr

A distributed MapReduce framework in Go. Language-agnostic — write your Map and Reduce functions in any language.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        Master                           │
│  HTTP API (:8080)              gRPC Control (:9090)     │
│  • POST /submit                • RegisterWorker         │
│  • GET  /status                • Heartbeat (pull-based  │
│  • DELETE /jobs/{id}             task assignment)        │
└──────────┬──────────────────────────┬───────────────────┘
           │                          │
    ┌──────┴──────┐            ┌──────┴──────┐
    │  Worker A   │            │  Worker B   │
    │  HTTP :8081 │◄──────────►│  HTTP :8082 │
    │  (partitions)            │  (partitions)│
    └─────────────┘            └──────────────┘
           │                          │
    ┌──────┴──────────────────────────┴──────┐
    │         S3-Compatible Storage          │
    │  (input data, source files, output)    │
    └────────────────────────────────────────┘
```

- **Master** coordinates jobs, tracks workers, assigns tasks via heartbeat responses.
- **Workers** register with the master, execute map/reduce tasks as child processes, serve intermediate data over HTTP.
- **S3** stores input data, map/reduce source files, and final output.

## I/O Contract

Map and reduce programs communicate via stdin/stdout:

| Program | stdin | stdout |
|---------|-------|--------|
| **Map** | Raw input data (streamed from S3) | `key\tvalue\n` lines |
| **Reduce** | Sorted `key\tvalue\n` lines (same key grouped) | `key\tvalue\n` lines |

The framework handles partitioning (`hash(key) % R`) and sorting — your code just processes data.

## Quick Start

### Prerequisites

- Go 1.21+
- S3-compatible storage (MinIO, Garage, AWS S3)
- Language toolchain for your map/reduce programs (e.g., Python, Go, Rust)

### Build

```bash
make build
```

### Run Master

```bash
./bin/gomr master
```

### Run Worker (in another terminal)

```bash
./bin/gomr worker
```

### Submit a Job

```bash
curl -X POST http://localhost:8080/submit \
  -H 'Content-Type: application/json' \
  -d '{
    "map_source_uri": "s3://thia/plugins/wordcount/mapper.py",
    "reduce_source_uri": "s3://thia/plugins/wordcount/reducer.py",
    "map_compile_cmd": "chmod +x {source} && cp {source} {output}",
    "reduce_compile_cmd": "chmod +x {source} && cp {source} {output}",
    "input_prefix": "s3://thia/data/wordcount/",
    "output_prefix": "s3://thia/output/wordcount/",
    "reduce_tasks": 3
  }'
```

### Check Status

```bash
curl http://localhost:8080/status
```

### Cancel a Job

```bash
curl -X DELETE http://localhost:8080/jobs/<job-id>
```

## Configuration

All config is in `config.toml`:

```toml
# Master
master_http_port = 8080
master_grpc_port = 9090

# Worker
master_grpc_addr = "localhost:9090"
worker_host = "localhost"
worker_http_port = 8081
worker_heartbeat_interval = "5s"

# Intermediate storage: partitions below this threshold (MB) stay in memory.
intermediate_spill_threshold_mb = 256

# S3
s3_endpoint = "thia:3900"
aws_access_key_id = "your-key"
aws_secret_access_key = "your-secret"
aws_region = "garage"
```

## Writing Map/Reduce Programs

### Python Example (wordcount)

**mapper.py:**
```python
#!/usr/bin/env python3
import sys
for line in sys.stdin:
    for word in line.strip().split():
        print(f"{word.lower()}\t1")
```

**reducer.py:**
```python
#!/usr/bin/env python3
import sys
current_key, count = None, 0
for line in sys.stdin:
    key, value = line.strip().split('\t', 1)
    if key == current_key:
        count += int(value)
    else:
        if current_key is not None:
            print(f"{current_key}\t{count}")
        current_key, count = key, int(value)
if current_key is not None:
    print(f"{current_key}\t{count}")
```

### Compiled Language Example (Go)

For compiled languages, the compile command does the actual compilation:

```json
{
  "map_source_uri": "s3://thia/plugins/wordcount/mapper.go",
  "map_compile_cmd": "go build -o {output} {source}",
  ...
}
```

## Project Structure

```
gomr/
├── cmd/gomr/          # Entry point (master/worker CLI)
├── internal/
│   ├── config/        # TOML configuration loading
│   ├── master/        # Master: HTTP API, gRPC control plane, job/task management
│   └── worker/        # Worker: registration, heartbeats, task execution
├── proto/gomr/v1/     # gRPC/protobuf definitions
├── examples/          # Example map/reduce programs
├── docs/              # Design documentation
├── config.toml        # Configuration file
└── Makefile           # Build, test, proto generation
```

## Development

```bash
make fmt          # Format code
make vet          # Run go vet
make test         # Run tests
make proto        # Regenerate protobuf bindings
make build        # Build binary
```
