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
)

// TaskAttempt tracks a single execution attempt of a task.
// Multiple attempts can exist for retries and speculative execution.
type TaskAttempt struct {
	AttemptID string    `json:"attempt_id"`
	WorkerID  string    `json:"worker_id"`
	StartedAt time.Time `json:"started_at"`
}

type MapTask struct {
	ID       string     `json:"id"`
	InputURI string     `json:"input_uri"`
	Status   TaskStatus `json:"status"`

	// Attempts tracks all execution attempts (supports retries + speculation).
	Attempts []*TaskAttempt `json:"attempts,omitempty"`

	// PartitionURLs is populated on completion — one HTTP URL per reduce partition,
	// pointing to the winning worker's partition data.
	PartitionURLs []string `json:"partition_urls,omitempty"`
}

type ReduceTask struct {
	ID        string     `json:"id"`
	Partition int        `json:"partition"` // 0-based index of the reduce partition this task handles.
	Status    TaskStatus `json:"status"`

	Attempts []*TaskAttempt `json:"attempts,omitempty"`

	// Set on completion:
	WinningAttemptID string `json:"winning_attempt_id,omitempty"` // AttemptID of the winning attempt.
	TempObject       string `json:"temp_object,omitempty"`        // S3 key of the temporary output object.
}

// --- Job Model ---

type JobStatus string

const (
	JobStatusQueued    JobStatus = "Queued"
	JobStatusMapping   JobStatus = "Mapping"
	JobStatusReducing  JobStatus = "Reducing"
	JobStatusCompleted JobStatus = "Completed"
	JobStatusFailed    JobStatus = "Failed"
	JobStatusAborted   JobStatus = "Aborted"
)

type Job struct {
	ID           string `json:"id"`
	InputPrefix  string `json:"input_prefix"`
	OutputPrefix string `json:"output_prefix"`

	MapSourceURI     string `json:"map_source_uri"`
	ReduceSourceURI  string `json:"reduce_source_uri"`
	MapCompileCmd    string `json:"map_compile_cmd"`
	ReduceCompileCmd string `json:"reduce_compile_cmd"`
	NumReduceTasks   int    `json:"num_reduce_tasks"`

	Status JobStatus `json:"status"`

	MapTasks        []*MapTask             `json:"map_tasks"`
	ReduceTasks     []*ReduceTask          `json:"reduce_tasks"`
	MapTaskIndex    map[string]*MapTask    `json:"-"` // rebuilt on restore
	ReduceTaskIndex map[string]*ReduceTask `json:"-"` // rebuilt on restore

	// Completion tracking for speculative execution thresholds.
	MapCompletionTimes    []time.Duration `json:"map_completion_times,omitempty"`
	ReduceCompletionTimes []time.Duration `json:"reduce_completion_times,omitempty"`
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

	// Checkpointing
	checkpointInterval time.Duration
	checkpointS3URI    string // e.g. "s3://bucket/gomr-checkpoints/"; empty disables checkpointing
}

// Checkpoint is the serializable snapshot of master state.
type Checkpoint struct {
	ActiveJobID string          `json:"active_job_id"`
	Jobs        map[string]*Job `json:"jobs"`
	SavedAt     time.Time       `json:"saved_at"`
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
