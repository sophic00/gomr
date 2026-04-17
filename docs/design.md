# Gomr: Go MapReduce Design Document

## 1. Overview

Gomr is a distributed MapReduce framework implemented in Go. It is distributed as a single standalone binary that can assume the role of either a Master or a Worker based on CLI arguments. Network security and authentication are deferred to Tailscale, allowing the system to operate securely without built-in auth complexity.

## 2. Architecture

### 2.1 Single Binary Execution

The application is compiled into a single executable `gomr`.

- **Master Mode:** Started via `gomr master --port <master_http_port>`. It runs as a persistent daemon, coordinating workers, managing a queue of submitted jobs, tracking task state, and handling fault tolerance. It exposes an HTTP API for job submission and status.
- **Worker Mode:** Started via `gomr worker --master <master_ip:port> --port <worker_http_port>`. It registers with the master, executes assigned map or reduce tasks, and serves intermediate files.

### 2.2 Network & Security

- **Tailscale:** All nodes run on a Tailscale tailnet. No application-level authentication or encryption is required.
- **RPC (Control Plane):** The Master and Workers communicate using Go's standard `net/rpc`. This is used for lightweight messaging: task assignment, status updates, and heartbeats.
- **HTTP (Data Plane & API):**
  - **Workers:** Spin up a standard Go `http.FileServer` to serve intermediate partition files.
  - **Master:** Exposes HTTP endpoints:
    - `POST /submit`: Accept new MapReduce jobs.
    - `GET /status`: Retrieve progress of active and queued jobs.
    - `DELETE /jobs/{id}`: Cancel a specific job.

## 3. Execution Model

### 3.1 Job Submission & Queue Semantics

Users submit jobs to the Master via an HTTP POST request containing a JSON payload. This payload specifies:

- The path/URL to the Map plugin (`.so` file).
- The path/URL to the Reduce plugin (`.so` file).
- The location of the input data (e.g., a shared network mount path like `/mnt/nfs/data` or an object storage URI).
- The desired location for the final output.
- The number of Reduce tasks ($R$).

**Queue Semantics & Greedy Concurrency:** The Master maintains a priority queue of submitted jobs based on FIFO (First-In-First-Out). However, to maximize cluster utilization, Gomr uses **Greedy Job Concurrency**. If the oldest job is temporarily bottlenecked (e.g., all its Map tasks are running, and it is waiting for them to finish before starting Reduce tasks), the Master will freely assign tasks from the next job in the queue to any idle workers. **Job Cancellation:** If a job is cancelled via the API, the Master removes it from the queue. If the cancelled job is currently running, the Master marks it as aborted, ignores any further task completions for it, and uses the worker's next heartbeat response to signal an abort of the in-flight task.

**Pull-Based Scheduling & Backpressure:** The Master does not actively push tasks to workers. Instead, it relies on a pull-based model piggybacked on standard RPC heartbeats. When a worker sends a heartbeat with a status of "Idle", the Master checks the job queue and replies to the heartbeat with a new task assignment. This naturally applies backpressure: the Master only gives work to workers that are proven to be alive and have explicit capacity, preventing overload.

**Bring Your Own Storage (BYOS) & Data Locality:** Gomr does not implement a distributed file system. It assumes that the input data, plugin files, and final output destinations provided in the job submission are globally accessible by all worker nodes (e.g., via an NFS mount over Tailscale, or cloud object storage). Because storage is entirely decoupled from compute in this model, **Gomr intentionally ignores data locality**; any worker is considered equally "close" to the input data.

### 3.2 Map Phase

1. Worker A sends an RPC heartbeat with status "Idle". The Master replies by assigning a Map task (e.g., process `chunk-1.txt`).
2. Worker A loads the `.so` plugin and runs the `Map` function.
3. Worker A partitions the intermediate key-value pairs into $R$ files (where $R$ is the number of reducers).
4. Worker A stores these files locally and serves them via its HTTP server.
5. Worker A reports completion to the Master via its next RPC heartbeat, including its HTTP address.

### 3.3 Reduce Phase

1. Worker B sends an RPC heartbeat with status "Idle". The Master replies by assigning a Reduce task (e.g., partition 5).
2. The Master provides Worker B with a list of HTTP URLs (pointing to the Map workers that hold partition 5).
3. Worker B downloads all partition 5 files using HTTP GET.
4. Worker B sorts the keys, runs the `.so` `Reduce` function, and writes the final output to a temporary file (e.g., `part-5.tmp`) on the shared storage.
5. **Atomic Rename:** Once the file is completely written and flushed to disk, Worker B performs an atomic `rename` from `part-5.tmp` to its final name (e.g., `part-5.txt`). This prevents partial or corrupted files from being left behind if a Reduce worker crashes mid-write.
6. Worker B reports completion to the Master via its next RPC heartbeat.

## 4. Fault Tolerance

- **Worker Identity & Registration:** Worker IDs are ephemeral and randomly generated on startup (e.g., a UUID). When a worker process crashes and restarts, it registers as a completely new worker. The Master does not attempt to map returning workers to past identities.
- **Heartbeats:** Workers ping the Master periodically via RPC. If the Master misses heartbeats for a configured timeout, the worker is marked as dead. Any in-progress tasks on a dead worker are reset to "idle" and reassigned.
- **Reduce Phase Completion Race:** If a Map worker dies after finishing its task, its HTTP server goes offline. If a Reduce worker tries to fetch files from it and fails (e.g., connection refused mid-transfer), the Reduce worker reports the failure to the Master. The Master then resets _both_ the failed Reduce task and the corresponding dead Map task back to "idle". The Map task will be reassigned to a new worker to regenerate the missing files, and the Reduce task will be retried once all Map outputs are available again.
- **Master Recovery (Checkpointing):** The Master takes periodic **snapshots (checkpoints)** of the cluster state (job queue, task statuses) and saves them as JSON to the shared storage. Snapshotting is preferred over Write-Ahead Logging (WAL) for its simplicity and bounded size. If the Master restarts, it loads the latest snapshot into memory to resume coordination. Since MapReduce tasks are idempotent, losing a few seconds of state between snapshots simply results in safe, redundant task re-execution.
- **Speculative Execution (Straggler Mitigation):** If a worker is running a task unusually slowly (e.g., due to a failing disk or high CPU load), it creates a bottleneck. If a job is near completion but waiting on a straggler, the Master will speculatively assign the same task to another idle worker that requests work via a heartbeat. Whichever worker completes the task first "wins," and the Master instructs the slower worker to abort.

## 5. Intermediate File Lifecycle & Cleanup

### 5.1 Isolated Worker Directories

To support multiple workers running safely on the same physical machine without data races, each worker instance operates within its own dedicated, ID-based temporary directory (e.g., `/tmp/gomr-worker-<worker-id>`).

### 5.2 Atomic Renames

When a Map worker generates an intermediate partition file, it first writes the data to a temporary file name (e.g., `mr-1-0.tmp`). Only after the file is completely written and explicitly flushed to disk does the worker perform an atomic `rename` operation to its final name (`mr-1-0.txt`). The HTTP `FileServer` only serves `.txt` files. This atomic operation guarantees that Reduce workers downloading the file via HTTP will never read a partial or corrupted file, even if the Map worker crashes mid-write.

### 5.3 Startup Cleanup

Workers rely on their process supervisor (e.g., `systemd` via the `RuntimeDirectory=` directive) to manage their temporary workspace. This ensures the operating system automatically provisions a fresh, empty directory on startup and cleanly destroys it when the worker exits or crashes. This delegates garbage collection entirely to the OS and prevents any orphaned intermediate files from persisting across worker restarts.

## 6. Future Considerations

### 6.1 WebAssembly (WASM)

Go's plugin system (`.so` files) has strict limitations (e.g., exact compiler version matches, lack of Windows support). In the future, the execution engine can be migrated to **WebAssembly (WASM)** using a runtime like `wazero`. This would allow Map and Reduce functions to be written in any language that compiles to WASM and execute in a safe, cross-platform sandbox.

### 6.2 gRPC Migration

Currently, the Control Plane uses `net/rpc` and the Data Plane uses `http.FileServer`. While efficient, it splits the network protocols. In the future, the system could be unified under **gRPC**. gRPC supports bidirectional streaming, which would allow both messaging and chunked file transfers over a single multiplexed connection, while also enabling cross-language workers.
