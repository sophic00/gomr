package worker

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// PartitionStore holds intermediate map output, distributed across R partitions.
// Data starts in memory and spills to disk if the total size exceeds the threshold.
type PartitionStore struct {
	mu sync.RWMutex

	numPartitions  int
	spillThreshold int // bytes; 0 = always in-memory
	totalSize      int
	spilled        bool

	buffers   []*bytes.Buffer // in-memory partitions (nil after spill)
	diskPaths []string        // set after spill

	// Metadata for HTTP serving.
	jobID  string
	taskID string
}

func partitionStoreKey(jobID, taskID string) string {
	return jobID + "/" + taskID
}

func parsePartitionRequestPath(path string) (jobID, taskID string, partIdx int, err error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 4 || parts[0] != "partitions" {
		return "", "", 0, fmt.Errorf("invalid partition URL")
	}

	partIdx, err = strconv.Atoi(parts[3])
	if err != nil || partIdx < 0 {
		return "", "", 0, fmt.Errorf("invalid partition index")
	}

	return parts[1], parts[2], partIdx, nil
}

// NewPartitionStore creates a new partition store for R partitions.
func NewPartitionStore(numPartitions int, spillThreshold int, jobID, taskID, workDir string) *PartitionStore {
	buffers := make([]*bytes.Buffer, numPartitions)
	for i := range buffers {
		buffers[i] = &bytes.Buffer{}
	}
	return &PartitionStore{
		numPartitions:  numPartitions,
		spillThreshold: spillThreshold,
		buffers:        buffers,
		diskPaths:      make([]string, numPartitions),
		jobID:          jobID,
		taskID:         taskID,
	}
}

// Write parses a key\tvalue line, hashes the key, and appends to the correct partition.
func (ps *PartitionStore) Write(line []byte) error {
	// Parse key from "key\tvalue\n" format.
	idx := bytes.IndexByte(line, '\t')
	if idx < 0 {
		return fmt.Errorf("malformed line (no tab separator): %q", line)
	}
	key := line[:idx]

	// Hash key to determine partition.
	h := fnv.New32a()
	h.Write(key)
	partition := int(h.Sum32()) % ps.numPartitions

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.spilled {
		// Append to disk file.
		f, err := os.OpenFile(ps.diskPaths[partition], os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to append to partition file %d: %w", partition, err)
		}
		_, err = f.Write(line)
		f.Close()
		if err != nil {
			return err
		}
		// Ensure line ends with newline.
		if len(line) > 0 && line[len(line)-1] != '\n' {
			f2, _ := os.OpenFile(ps.diskPaths[partition], os.O_APPEND|os.O_WRONLY, 0644)
			f2.Write([]byte{'\n'})
			f2.Close()
		}
		return nil
	}

	// Write to in-memory buffer.
	ps.buffers[partition].Write(line)
	if len(line) > 0 && line[len(line)-1] != '\n' {
		ps.buffers[partition].WriteByte('\n')
	}
	ps.totalSize += len(line)

	// Check if we need to spill.
	if ps.spillThreshold > 0 && ps.totalSize >= ps.spillThreshold {
		return ps.spillLocked()
	}
	return nil
}

// spillLocked writes all in-memory buffers to disk. Must be called with ps.mu held.
func (ps *PartitionStore) spillLocked() error {
	dir := filepath.Join(os.TempDir(), "gomr-partitions", ps.jobID, ps.taskID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create spill directory: %w", err)
	}

	for i, buf := range ps.buffers {
		path := filepath.Join(dir, fmt.Sprintf("partition-%d", i))
		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			return fmt.Errorf("failed to spill partition %d: %w", i, err)
		}
		ps.diskPaths[i] = path
		ps.buffers[i] = nil // Free memory.
	}

	ps.spilled = true
	ps.buffers = nil
	slog.Info("partitions spilled to disk",
		"job_id", ps.jobID,
		"task_id", ps.taskID,
		"total_size_mb", ps.totalSize/(1024*1024),
		"dir", dir,
	)
	return nil
}

// URLs returns the HTTP URLs for all partitions, relative to the worker's address.
func (ps *PartitionStore) URLs(workerHTTPAddr string) []string {
	urls := make([]string, ps.numPartitions)
	for i := 0; i < ps.numPartitions; i++ {
		urls[i] = fmt.Sprintf("http://%s/partitions/%s/%s/%d",
			workerHTTPAddr, ps.jobID, ps.taskID, i)
	}
	return urls
}

// ServePartition handles an HTTP request for a specific partition.
// URL pattern: /partitions/{job_id}/{task_id}/{partition_index}
func (ps *PartitionStore) ServePartition(w http.ResponseWriter, r *http.Request) {
	jobID, taskID, partIdx, err := parsePartitionRequestPath(r.URL.Path)
	if err != nil {
		http.Error(w, "invalid partition URL", http.StatusBadRequest)
		return
	}
	if jobID != ps.jobID || taskID != ps.taskID {
		http.Error(w, "partition not available", http.StatusNotFound)
		return
	}
	if partIdx >= ps.numPartitions {
		http.Error(w, "invalid partition index", http.StatusBadRequest)
		return
	}

	ps.mu.RLock()
	defer ps.mu.RUnlock()

	w.Header().Set("Content-Type", "text/plain")

	if ps.spilled {
		http.ServeFile(w, r, ps.diskPaths[partIdx])
		return
	}

	if ps.buffers != nil && ps.buffers[partIdx] != nil {
		w.Write(ps.buffers[partIdx].Bytes())
		return
	}

	http.Error(w, "partition not available", http.StatusNotFound)
}

// Cleanup frees memory and deletes disk files.
func (ps *PartitionStore) Cleanup() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.buffers = nil
	if ps.spilled {
		dir := filepath.Dir(ps.diskPaths[0])
		os.RemoveAll(dir)
	}
}
