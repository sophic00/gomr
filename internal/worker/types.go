package worker

import (
	"context"
	"sync"

	"github.com/minio/minio-go/v7"
	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
)

// Worker represents the local state of a Gomr worker instance.
type Worker struct {
	mu sync.RWMutex

	ID             string
	MasterGRPCAddr string
	HTTPAddr       string

	State       gomrv1.WorkerState
	CurrentTask *gomrv1.TaskRef

	// S3 client for downloading source files, input data, and uploading output.
	s3Client *minio.Client

	// lastResult holds the outcome of the most recently completed task.
	// It is sent to the master on the next heartbeat, then cleared.
	lastResult *gomrv1.TaskResult

	// cancelTask cancels the currently running task's context.
	cancelTask context.CancelFunc

	// partitions holds the current map task's partition data, served via HTTP.
	partitions *PartitionStore

	// workDir is a temp directory for compiled binaries and spill files.
	workDir string

	// spillThreshold is the max bytes to hold in memory before spilling to disk.
	spillThreshold int

	// reduceUpdates receives HeartbeatResponse updates with additional reduce URLs.
	reduceUpdates chan *gomrv1.HeartbeatResponse
}
