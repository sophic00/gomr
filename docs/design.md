# Gomr: Go MapReduce Design Document

## 1. Overview

Gomr is a distributed MapReduce framework implemented in Go. It is distributed as a single standalone binary that can assume the role of either a Master or a Worker based on CLI arguments. Network security and authentication are deferred to Tailscale, allowing the system to operate securely without built-in auth complexity.

## 2. Architecture

### 2.1 Single Binary Execution

The application is compiled into a single executable `gomr`.

- **Master Mode:** Started via `gomr master`. It runs as a persistent daemon, coordinating workers, managing a queue of submitted jobs, tracking task state, and handling fault tolerance. It loads its configuration (such as HTTP and gRPC ports) from `config.toml`. It exposes an HTTP API for job submission and status, and a gRPC control-plane endpoint for workers.
- **Worker Mode:** Started via `gomr worker`. It loads its configuration (such as the Master's gRPC address and its own HTTP port) from `config.toml`. It registers with the master over gRPC, executes assigned map or reduce tasks, and serves intermediate files over HTTP.

### 2.2 Network & Security

- **Tailscale:** All nodes run on a Tailscale tailnet. No application-level authentication or encryption is required.
- **gRPC (Control Plane):** The Master and Workers communicate over **gRPC**. This is used for lightweight messaging: task assignment, status updates, and heartbeats.
- **HTTP (Data Plane & API):**
  - **Workers:** Spin up a standard Go `http.FileServer` to serve intermediate partition files. Additionally, the worker exposes a `GET /health` endpoint for the Master to verify its reachability during registration.
  - **Master:** Exposes HTTP endpoints:
    - `POST /submit`: Accept new MapReduce jobs.
    - `GET /status`: Retrieve progress of active and queued jobs.
    - `DELETE /jobs/{id}`: Cancel a specific job.

## 3. Execution Model

### 3.1 Job Submission & Queue Semantics

Users submit jobs to the Master via an HTTP POST request containing a JSON payload. This payload specifies:

- The path/URL to the Map plugin (`.so` file).
- The path/URL to the Reduce plugin (`.so` file).
- The location of the input data (e.g., an S3 prefix like `s3://bucket/data/`).
- The desired location for the final output.
- The number of Map and Reduce tasks ($M$, $R$).

**Data Splitting:** Gomr expects input data to be pre-split. The user or an external ingestion pipeline must divide the dataset into multiple smaller objects under the specified input prefix in the S3-compatible storage (e.g., `data/chunk-1.txt`, `data/chunk-2.txt`). When the Master receives a job, it lists the objects under the input prefix. Each object becomes exactly one Map task. The Master does not download or split the files itself; it simply assigns the object URIs as tasks to the workers, and the workers download the files directly from S3.

**Queue Semantics:** The Master maintains a strict FIFO (First-In-First-Out) queue for submitted jobs. To simplify resource allocation, jobs are processed sequentially (one active job at a time).

**Job IDs & Cancellation:** Each job is assigned a unique Job ID upon submission. When a worker is assigned a task, it receives the corresponding Job ID. The worker then includes this Job ID in all subsequent heartbeats (while busy) and task completion reports. If a job is cancelled via the API (`DELETE /jobs/{id}`), the Master removes it from the queue. If the cancelled job is currently running, the Master marks it as aborted. When the Master receives a heartbeat or completion report referencing an aborted Job ID, the Master uses the heartbeat response to explicitly instruct the worker to immediately abort its in-flight task, delete its local working directory, and return to an "Idle" state.

Crucially, because a job can be cancelled _after_ some workers have already completed their tasks and returned to an "Idle" state, the Master cannot rely solely on heartbeat abort signals to clean up. Therefore, immediately upon receiving the `DELETE` request, the Master itself must execute a cleanup routine to explicitly delete the job's intermediate and final output prefixes from the S3-compatible storage.

**Pull-Based Scheduling & Backpressure:** The Master does not actively push tasks to workers. Instead, it relies on a pull-based model piggybacked on gRPC heartbeats. When a worker sends a heartbeat with a status of "Idle", the Master checks the job queue and replies to the heartbeat with a new task assignment. This naturally applies backpressure: the Master only gives work to workers that are proven to be alive and have explicit capacity, preventing overload.

**Bring Your Own Storage (BYOS) & Data Locality:** Gomr does not implement a distributed file system, nor does the Master host a file server for the `.so` plugins. It assumes that the input data, the `.so` plugin files, and the final output destinations are stored in **S3-compatible object storage** (e.g., AWS S3, MinIO, GCS) accessible by all worker nodes. Because storage is entirely decoupled from compute in this model, **Gomr intentionally ignores data locality**; any worker is considered equally "close" to the input data.

**Intermediate Storage:** Unlike input and output data, intermediate files generated during the Map phase are stored on the **worker's local storage** and served directly by that worker via HTTP. This avoids the overhead of writing transient data to object storage.

### 3.2 Map Phase

1. Worker A sends a gRPC heartbeat with status "Idle". The Master replies by assigning a Map task (e.g., process `chunk-1.txt`).
2. Worker A loads the `.so` plugin and runs the `Map` function.
3. Worker A partitions the intermediate key-value pairs into $R$ files (where $R$ is the number of reducers).
4. Worker A stores these files locally and serves them via its HTTP server.
5. Worker A reports completion to the Master via its next gRPC heartbeat, including its HTTP address.

### 3.3 Reduce Phase

1. Worker B sends a gRPC heartbeat with status "Idle". The Master replies by assigning a Reduce task (e.g., partition 5).
2. The Master provides Worker B with a list of HTTP URLs (pointing to the Map workers that hold partition 5).
3. Worker B downloads all partition 5 files using HTTP GET.
4. Worker B sorts the keys, runs the `.so` `Reduce` function, and writes the final output to a temporary, task-attempt-scoped object (e.g., `part-5-<attempt-id>.tmp`) in the **S3-compatible object storage**. The `<attempt-id>` is a UUID randomly generated by the Worker when it begins the task. Using a worker-generated, attempt-specific ID prevents data corruption if multiple workers execute the same task speculatively.
5. **Finalize Output:** Once the object is fully written, Worker B reports completion to the Master with its `<attempt-id>`. The Master checks if the task is already completed; if not, it blesses this attempt as the winner. The "winning" worker is instructed (in the gRPC response) to promote the file. Since S3 lacks an atomic rename operation, the worker must explicitly perform a `CopyObject` operation from the `.tmp` key to the final `.txt` key, followed by a `DeleteObject` to remove the `.tmp` key.
6. Worker B acknowledges successful promotion to the Master via its next gRPC heartbeat.

## 4. Fault Tolerance

- **Worker Identity & Registration:** Worker IDs are ephemeral and randomly generated on startup (e.g., a UUID). When a worker process crashes and restarts, it registers as a completely new worker. During this registration, the Master actively verifies the worker's self-reported HTTP address by issuing a lightweight probe (e.g., a `GET /health` request). If the worker is unreachable (e.g., due to NAT or firewall issues), the Master rejects the registration, preventing dead URLs from being handed to Reduce workers later. The Master does not attempt to map returning workers to past identities.
- **Heartbeats:** Workers ping the Master periodically via gRPC. If the Master misses heartbeats for a configured timeout, the worker is marked as dead. Any in-progress tasks on a dead worker are reset to "idle" and reassigned.
- **Reduce Phase Completion Race:** If a Map worker dies after finishing its task, its HTTP server goes offline. If a Reduce worker tries to fetch files from it and fails (e.g., connection refused mid-transfer), the Reduce worker reports the failure to the Master. The Master then resets _both_ the failed Reduce task and the corresponding dead Map task back to "idle". The Map task will be reassigned to a new worker to regenerate the missing files, and the Reduce task will be retried once all Map outputs are available again.
- **Master Recovery (Checkpointing):** The Master takes periodic **snapshots (checkpoints)** of the cluster state (job queue, task statuses) and saves them as JSON to the **S3-compatible object storage**. To prevent checkpoint corruption if the Master crashes mid-write, it uses the safe S3 write pattern: write to a temporary key (e.g., `checkpoint-<timestamp>.tmp`), execute a `CopyObject` to the final key, and then issue a `DeleteObject` for the temporary key. Furthermore, the Master keeps the last N checkpoints instead of a single file, allowing it to fall back to an older state if the newest checkpoint is compromised. Snapshotting is preferred over Write-Ahead Logging (WAL) for its simplicity and bounded size. If the Master restarts, it loads the latest valid snapshot into memory to resume coordination. Since MapReduce tasks are idempotent, losing a few seconds of state between snapshots simply results in safe, redundant task re-execution.
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

### 6.2 Expanded gRPC Usage

Currently, the Control Plane uses **gRPC** and the Data Plane uses `http.FileServer`. While efficient, it still splits the network protocols. In the future, the system could move more of the Data Plane under **gRPC** as well. gRPC supports bidirectional streaming, which would allow both messaging and chunked file transfers over a single multiplexed connection, while also enabling cross-language workers.

### 6.3 Pipelined Execution

Currently, the framework imposes a hard synchronization barrier: all Map tasks must complete before any Reduce task is assigned. In the future, execution could be pipelined. A Reduce task for partition `k` could begin as soon as all Map tasks that contribute specifically to partition `k` are complete, reducing the overall latency of the job.

### 6.4 Even Task Sizing

Currently, "each S3 object becomes one Map task". This means task sizing is entirely dependent on how the operator pre-split the data. A 10MB chunk and a 10GB chunk are both processed as single tasks, which can lead to severe straggler issues. While currently an operator responsibility, future work could implement a size-based logical splitting mechanism (e.g., using S3 `Range` requests) to ensure uniform task durations regardless of the underlying object sizes.
