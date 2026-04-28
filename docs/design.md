# Gomr: Go MapReduce Design Document

## 1. Overview

Gomr is a distributed MapReduce framework implemented in Go. It is distributed as a single standalone binary that can assume the role of either a Master or a Worker based on CLI arguments. Network security and authentication are deferred to Tailscale, allowing the system to operate securely without built-in auth complexity.

## 2. Architecture

### 2.1 Single Binary Execution

The application is compiled into a single executable `gomr`.

- **Master Mode:** Started via `gomr master`. It runs as a persistent daemon, coordinating workers, managing a queue of submitted jobs, tracking task state, and handling fault tolerance. It loads its configuration from `config.toml`. It exposes an HTTP API for job submission and status, and a gRPC control-plane endpoint for workers.
- **Worker Mode:** Started via `gomr worker`. It loads its configuration from `config.toml`. It registers with the master over gRPC, executes assigned map or reduce tasks as child processes, and serves intermediate data over HTTP.

### 2.2 Network & Security

- **Tailscale:** All nodes run on a Tailscale tailnet. No application-level authentication or encryption is required.
- **gRPC (Control Plane):** The Master and Workers communicate over **gRPC**. This is used for lightweight messaging: task assignment, status updates, and heartbeats.
- **HTTP (Data Plane & API):**
  - **Workers:** Serve intermediate partition data via HTTP for reduce workers to download. The worker also exposes a `GET /health` endpoint for the Master to verify its reachability during registration.
  - **Master:** Exposes HTTP endpoints:
    - `POST /submit`: Accept new MapReduce jobs.
    - `GET /status`: Retrieve progress of active and queued jobs.
    - `DELETE /jobs/{id}`: Cancel a specific job.

## 3. Execution Model

### 3.1 Language-Agnostic Child Process Model

Gomr uses a **child process execution model** rather than Go plugins. Map and Reduce functions are supplied as **source code files** in any language. The worker downloads them from S3, compiles them using user-provided compile commands, and runs the resulting binaries as child processes.

**I/O Contract:**

- **Map program:** Reads input data from **stdin**, writes intermediate key-value pairs to **stdout** as `key\tvalue\n` lines (tab-separated, newline-delimited).
- **Reduce program:** Reads sorted `key\tvalue\n` lines from **stdin** (all values for the same key are consecutive), writes output `key\tvalue\n` lines to **stdout**.

**Partitioning:** The worker handles partitioning of map output. It reads the map child's stdout, hashes each key via `hash(key) % R` (where R is the number of reduce tasks), and distributes key-value pairs into R partition buffers. This keeps user code simple and language-agnostic. Custom partitioning functions may be supported in the future.

### 3.2 Job Submission & Queue Semantics

Users submit jobs to the Master via an HTTP POST request containing a JSON payload:

```json
{
  "map_source_uri": "s3://thia/plugins/wordcount/mapper.py",
  "reduce_source_uri": "s3://thia/plugins/wordcount/reducer.py",
  "map_compile_cmd": "chmod +x {source} && cp {source} {output}",
  "reduce_compile_cmd": "chmod +x {source} && cp {source} {output}",
  "input_prefix": "s3://thia/data/wordcount/",
  "output_prefix": "s3://thia/output/wordcount/",
  "reduce_tasks": 3
}
```

- `map_source_uri` / `reduce_source_uri`: S3 paths to the source files.
- `map_compile_cmd` / `reduce_compile_cmd`: Shell commands to compile the source into a runnable binary. `{source}` and `{output}` are template placeholders the worker substitutes at runtime.
- `input_prefix`: S3 prefix containing the pre-split input data files.
- `output_prefix`: S3 prefix where final output will be written.
- `reduce_tasks`: Number of reduce partitions (R).

**Data Splitting:** Gomr expects input data to be pre-split. The user or an external ingestion pipeline must divide the dataset into multiple smaller objects under the specified input prefix in S3-compatible storage. When the Master receives a job, it lists the objects under the input prefix. Each object becomes exactly one Map task.

**Queue Semantics:** The Master maintains a FIFO queue for submitted jobs. If the queue is full, the submission is rejected with `503 Service Unavailable`.

**Job IDs & Cancellation:** Each job is assigned a unique Job ID upon submission. If a job is cancelled via the API (`DELETE /jobs/{id}`), the Master marks it as aborted. The scheduler will skip aborted jobs. The Master also initiates background cleanup of S3 output data.

**Pull-Based Scheduling & Backpressure:** The Master does not actively push tasks to workers. Instead, it relies on a pull-based model piggybacked on gRPC heartbeats. When a worker sends a heartbeat with a status of "Idle", the Master checks the job queue and replies with a new task assignment. This naturally applies backpressure: the Master only gives work to workers that are proven to be alive and have explicit capacity.

### 3.3 Map Phase

1. Worker A sends a gRPC heartbeat with status "Idle". The Master replies by assigning a Map task (e.g., process `chunk-1.txt`).
2. Worker A downloads the map source file from S3 and compiles it using the provided compile command.
3. Worker A opens the input file from S3 as a **streaming reader** and pipes it directly to the compiled map binary's stdin.
4. Worker A reads `key\tvalue\n` lines from the child's stdout, hashes each key, and distributes the pair into one of R **in-memory partition buffers**.
5. If total partition data exceeds the configurable **spill threshold** (default: 256MB), the worker spills all partitions to disk files.
6. Worker A serves partition data via its HTTP server (from memory or disk, depending on whether a spill occurred).
7. Worker A reports completion to the Master via its next gRPC heartbeat, including its HTTP address.

### 3.4 Reduce Phase

1. Worker B sends a gRPC heartbeat with status "Idle". The Master replies by assigning a Reduce task (e.g., partition 5).
2. The Master provides Worker B with a list of HTTP URLs (pointing to the Map workers that hold partition 5).
3. Worker B downloads all partition 5 files using HTTP GET.
4. Worker B sorts the keys, pipes the sorted data to the compiled reduce binary's stdin, reads output from stdout, and writes the final output to a temporary, task-attempt-scoped object in S3 (e.g., `part-5-<attempt-id>.tmp`).
5. **Finalize Output:** Once the object is fully written, Worker B reports completion to the Master. The Master blesses the winning attempt and instructs the worker to promote the file via `CopyObject` + `DeleteObject` (since S3 lacks atomic rename).

## 4. Intermediate Storage

Intermediate data from the Map phase is stored on the worker and served via HTTP.

### 4.1 In-Memory with Spill-to-Disk

The worker maintains R in-memory `bytes.Buffer` partitions (one per reduce task). It tracks total memory usage across all partitions. If the cumulative size exceeds a configurable threshold (`intermediate_spill_threshold_mb` in `config.toml`, default 256MB), all partitions are spilled to disk files.

- **Below threshold:** Partition data is served from memory via HTTP.
- **Above threshold:** Partition data is served from disk via `http.ServeFile`.

### 4.2 No Atomic Renames Needed

Since intermediate data is either in-memory buffers or written completely before being served, there is no partial-read risk. The HTTP handler only serves data after the Map task has completed.

## 5. Fault Tolerance

- **Worker Identity & Registration:** Worker IDs are ephemeral and randomly generated on startup (UUIDs). When a worker crashes and restarts, it registers as a new worker. During registration, the Master probes the worker's `GET /health` endpoint to verify reachability.
- **Heartbeats:** Workers ping the Master periodically via gRPC. If the Master misses heartbeats for a configured timeout, the worker is marked as dead. Any in-progress tasks are reset to "idle" and reassigned.
- **Graceful Shutdown:** Both Master and Worker handle `SIGTERM`/`SIGINT` signals for graceful shutdown, draining in-flight requests before exiting.
- **Auto Re-registration:** If a worker's heartbeat receives a "not registered" error (e.g., after a master restart), the worker automatically re-registers.
- **Reduce Phase Failure Recovery:** If a Reduce worker fails to fetch files from a dead Map worker, it reports the failure. The Master resets both the failed Reduce task and the corresponding Map task.
- **Master Recovery (Checkpointing):** The Master takes periodic snapshots of cluster state and saves them to S3. On restart, it loads the latest valid snapshot. Since MapReduce tasks are idempotent, losing state between snapshots results in safe, redundant re-execution.
- **Speculative Execution (Future):** If a task is running unusually slowly, the Master may speculatively assign the same task to another idle worker. Whichever worker completes first "wins."

## 6. Configuration

All configuration is via a single `config.toml` file:

```toml
# Master
master_http_port = 8080
master_grpc_port = 9090

# Worker
master_grpc_addr = "localhost:9090"
worker_host = "localhost"
worker_http_port = 8081
worker_heartbeat_interval = "5s"

# Intermediate storage
intermediate_spill_threshold_mb = 256

# S3
s3_endpoint = "thia:3900"
aws_access_key_id = "..."
aws_secret_access_key = "..."
aws_region = "garage"
```

## 7. Future Considerations

### 7.1 Distributed File System with Offset Calculator

Allow users to provide an offset calculator where the Master calculates the offset of each split and sends the worker those offsets, so workers can read that range of data using S3 `Range` requests. This enables processing large files without requiring pre-splitting.

### 7.2 Prefetch

Workers will be able to prefetch the next task's data from S3 or Map workers while the current task is still running, reducing idle time between tasks.

### 7.3 Concurrent Job Execution

Currently, jobs are processed sequentially. Future work will enable multiple jobs to execute simultaneously, with configurable resource allocation per job.

### 7.4 Custom Partitioning Functions

Allow users to supply custom hash/partitioning functions to control how map output is distributed across reduce tasks.

### 7.5 Pipelined Execution

A Reduce task for partition `k` could begin as soon as all Map tasks that contribute to partition `k` are complete, rather than waiting for all Map tasks to finish.
