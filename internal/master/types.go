package master

import (
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
)

// --- Job Submission (HTTP API) ---

// JobSubmission represents the expected JSON payload for POST /submit
type JobSubmission struct {
	MapSourceURI     string `json:"map_source_uri"`
	ReduceSourceURI  string `json:"reduce_source_uri"`
	MapCompileCmd    string `json:"map_compile_cmd"`
	ReduceCompileCmd string `json:"reduce_compile_cmd"`
	InputPrefix      string `json:"input_prefix"`
	OutputPrefix     string `json:"output_prefix"`
	ReduceTasks      int    `json:"reduce_tasks"`
}

type JobSubmitResponse struct {
	JobID            string `json:"job_id"`
	Status           string `json:"status"`
	Message          string `json:"message"`
	MapTasksCount    int    `json:"map_tasks_count"`
	ReduceTasksCount int    `json:"reduce_tasks_count"`
	QueueSize        int    `json:"queue_size"`
}

// --- Task Model ---

type TaskStatus string

const (
	TaskStatusIdle       TaskStatus = "Idle"
	TaskStatusInProgress TaskStatus = "InProgress"
	TaskStatusCompleted  TaskStatus = "Completed"
	TaskStatusFailed     TaskStatus = "Failed"
)

// TaskAttempt tracks a single execution attempt of a task.
// Multiple attempts can exist for retries and speculative execution.
type TaskAttempt struct {
	AttemptID string
	WorkerID  string
	StartedAt time.Time
}

type MapTask struct {
	ID       string
	InputURI string
	Status   TaskStatus

	// Attempts tracks all execution attempts (supports retries + speculation).
	Attempts []*TaskAttempt

	// PartitionURLs is populated on completion — one HTTP URL per reduce partition,
	// pointing to the winning worker's partition data.
	PartitionURLs []string
}

type ReduceTask struct {
	ID        string
	Partition int // 0-based index of the reduce partition this task handles.
	Status    TaskStatus

	Attempts []*TaskAttempt

	// Set on completion:
	WinningAttemptID string // AttemptID of the winning attempt.
	TempObject       string // S3 key of the temporary output object.
}

// --- Job Model ---

type JobStatus string

const (
	JobStatusQueued     JobStatus = "Queued"
	JobStatusMapping    JobStatus = "Mapping"
	JobStatusReducing   JobStatus = "Reducing"
	JobStatusCompleted  JobStatus = "Completed"
	JobStatusFailed     JobStatus = "Failed"
	JobStatusAborted    JobStatus = "Aborted"
)

type Job struct {
	ID           string
	InputPrefix  string
	OutputPrefix string

	MapSourceURI     string
	ReduceSourceURI  string
	MapCompileCmd    string
	ReduceCompileCmd string
	NumReduceTasks   int

	Status JobStatus

	MapTasks        []*MapTask
	ReduceTasks     []*ReduceTask
	MapTaskIndex    map[string]*MapTask
	ReduceTaskIndex map[string]*ReduceTask

	// Completion tracking for speculative execution thresholds.
	MapCompletionTimes    []time.Duration
	ReduceCompletionTimes []time.Duration
}

// --- Status API ---

type JobStatusInfo struct {
	JobID          string    `json:"job_id"`
	Status         JobStatus `json:"status"`
	MapProgress    string    `json:"map_progress"`
	ReduceProgress string    `json:"reduce_progress"`
}

type SystemStatusResponse struct {
	Workers map[string]int  `json:"workers"`
	Jobs    []JobStatusInfo `json:"jobs"`
}

// --- Master ---

type Master struct {
	httpPort int
	grpcPort int

	mu      sync.RWMutex
	jobs    map[string]*Job
	workers map[string]*Worker
	queue   chan string

	// activeJobID is the currently executing job. Empty means pick from queue.
	activeJobID string

	s3Client          *minio.Client
	httpClient        *http.Client
	heartbeatInterval time.Duration
	workerTimeout     time.Duration
}

// --- Worker (master-side representation) ---

type Worker struct {
	ID            string
	HTTPAddr      string
	State         gomrv1.WorkerState
	RegisteredAt  time.Time
	LastHeartbeat time.Time
	CurrentTask   *gomrv1.TaskRef
}
