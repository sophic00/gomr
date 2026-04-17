# Gomr: Go MapReduce Design Document

## 1. Overview

Gomr is a distributed MapReduce framework implemented in Go. It is distributed as a single standalone binary that can assume the role of either a Master or a Worker based on CLI arguments. Network security and authentication are deferred to Tailscale, allowing the system to operate securely without built-in auth complexity.

## 2. Architecture

### 2.1 Single Binary Execution

The application is compiled into a single executable `gomr`.

- **Master Mode:** Started via `gomr master --map <map_plugin.so> --reduce <reduce_plugin.so>`. It coordinates workers, tracks task state, and handles fault tolerance.
- **Worker Mode:** Started via `gomr worker --master <master_ip:port> --port <worker_http_port>`. It registers with the master, executes assigned map or reduce tasks, and serves intermediate files.

### 2.2 Network & Security

- **Tailscale:** All nodes run on a Tailscale tailnet. No application-level authentication or encryption is required.
- **RPC (Control Plane):** The Master and Workers communicate using Go's standard `net/rpc`. This is used for lightweight messaging: task assignment, status updates, and heartbeats.
- **HTTP (Data Plane):** Workers spin up a standard Go `http.FileServer` to serve intermediate partition files. When a Reduce worker needs a partition, it fetches it via standard HTTP GET requests from the respective Map worker's HTTP server.

## 3. Execution Model

### 3.1 Plugin System

Map and Reduce functions are provided to the Master at startup via Go's `buildmode=plugin` (`.so` files). The Master distributes the names/paths of these plugins to the workers. Workers dynamically load the `.so` files at runtime to execute the user-defined logic.

### 3.2 Map Phase

1. Master assigns a Map task (e.g., process `chunk-1.txt`) to Worker A.
2. Worker A loads the `.so` plugin and runs the `Map` function.
3. Worker A partitions the intermediate key-value pairs into $R$ files (where $R$ is the number of reducers).
4. Worker A stores these files locally and serves them via its HTTP server.
5. Worker A reports completion to the Master via RPC, including its HTTP address.

### 3.3 Reduce Phase

1. Master assigns a Reduce task (e.g., partition 5) to Worker B.
2. Master provides Worker B with a list of HTTP URLs (pointing to the Map workers that hold partition 5).
3. Worker B downloads all partition 5 files using HTTP GET.
4. Worker B sorts the keys, runs the `.so` `Reduce` function, and writes the final output to a file.
5. Worker B reports completion to the Master.

## 4. Fault Tolerance

- **Heartbeats:** Workers ping the Master periodically via RPC. If the Master misses heartbeats for a configured timeout, the worker is marked as dead.
- **Reassignment:** Any in-progress tasks on a dead worker are reset to "idle" and reassigned to other available workers.
- **Map Output Loss:** If a Map worker dies after completing its task but before Reducers have fetched its files, the HTTP server becomes unreachable. The Reducer will fail the HTTP fetch, report the error to the Master, and the Master will re-queue the corresponding Map task.
- **Worker Restarts:** Process-level crashes and restarts on individual machines are managed by external process supervisors like `systemd`. When a worker process is restarted by systemd, it registers with the Master as a new worker instance.

## 5. Intermediate File Lifecycle & Cleanup

### 5.1 Isolated Worker Directories

To support multiple workers running safely on the same physical machine without data races, each worker instance operates within its own dedicated, ID-based temporary directory (e.g., `/tmp/gomr-worker-<worker-id>`).

### 5.2 Atomic Renames

When a Map worker generates an intermediate partition file, it first writes the data to a temporary file name (e.g., `mr-1-0.tmp`). Only after the file is completely written and explicitly flushed to disk does the worker perform an atomic `rename` operation to its final name (`mr-1-0.txt`). The HTTP `FileServer` only serves `.txt` files. This atomic operation guarantees that Reduce workers downloading the file via HTTP will never read a partial or corrupted file, even if the Map worker crashes mid-write.

### 5.3 Startup Cleanup

When a worker process starts (or is restarted by `systemd`), its first action is to completely wipe its specific isolated temporary directory `/tmp/gomr-worker-<worker-id>` (if it exists) and recreate it. This guarantees a clean slate, instantly garbage-collecting any orphaned or half-written files left behind by a previous crash of that specific worker instance, without affecting any other workers running concurrently on the same machine.

## 6. Future Considerations

### 6.1 WebAssembly (WASM)

Go's plugin system (`.so` files) has strict limitations (e.g., exact compiler version matches, lack of Windows support). In the future, the execution engine can be migrated to **WebAssembly (WASM)** using a runtime like `wazero`. This would allow Map and Reduce functions to be written in any language that compiles to WASM and execute in a safe, cross-platform sandbox.

### 6.2 gRPC Migration

Currently, the Control Plane uses `net/rpc` and the Data Plane uses `http.FileServer`. While efficient, it splits the network protocols. In the future, the system could be unified under **gRPC**. gRPC supports bidirectional streaming, which would allow both messaging and chunked file transfers over a single multiplexed connection, while also enabling cross-language workers.
