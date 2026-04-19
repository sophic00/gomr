package worker

import (
	"sync"

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
}
