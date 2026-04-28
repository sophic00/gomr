package master

import (
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
)

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

type TaskStatus string

const (
	TaskStatusIdle       TaskStatus = "Idle"
	TaskStatusInProgress TaskStatus = "InProgress"
	TaskStatusCompleted  TaskStatus = "Completed"
	TaskStatusFailed     TaskStatus = "Failed"
)

type MapTask struct {
	ID       string
	InputURI string
	Status   TaskStatus
	WorkerID string
}

type ReduceTask struct {
	ID       string
	Status   TaskStatus
	WorkerID string
}

type JobStatus string

const (
	JobStatusQueued     JobStatus = "Queued"
	JobStatusInProgress JobStatus = "InProgress"
	JobStatusCompleted  JobStatus = "Completed"
	JobStatusFailed     JobStatus = "Failed"
	JobStatusAborted    JobStatus = "Aborted"
)

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

type Master struct {
	httpPort int
	grpcPort int

	mu      sync.RWMutex
	jobs    map[string]*Job
	workers map[string]*Worker
	queue   chan string

	s3Client          *minio.Client
	httpClient        *http.Client
	heartbeatInterval time.Duration
	workerTimeout     time.Duration
}

type Job struct {
	ID           string
	InputPrefix  string
	OutputPrefix string

	MapSourceURI    string
	ReduceSourceURI string
	MapCompileCmd   string
	ReduceCompileCmd string
	NumReduceTasks  int

	Status JobStatus

	MapTasks         []*MapTask
	ReduceTasks      []*ReduceTask
	MapTaskIndex     map[string]*MapTask
	ReduceTaskIndex  map[string]*ReduceTask
}

type JobSubmitResponse struct {
	JobID            string `json:"job_id"`
	Status           string `json:"status"`
	Message          string `json:"message"`
	MapTasksCount    int    `json:"map_tasks_count"`
	ReduceTasksCount int    `json:"reduce_tasks_count"`
	QueueSize        int    `json:"queue_size"`
}

type Worker struct {
	ID            string
	HTTPAddr      string
	State         gomrv1.WorkerState
	RegisteredAt  time.Time
	LastHeartbeat time.Time
	CurrentTask   *gomrv1.TaskRef
}
