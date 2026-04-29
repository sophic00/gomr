package master

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

type controlServer struct {
	gomrv1.UnimplementedMasterServiceServer
	master *Master
}

func (s *controlServer) RegisterWorker(ctx context.Context, req *gomrv1.RegisterWorkerRequest) (*gomrv1.RegisterWorkerResponse, error) {
	if req.GetWorkerId() == "" {
		return &gomrv1.RegisterWorkerResponse{
			Accepted: false,
			Message:  "worker_id is required",
		}, nil
	}
	if req.GetHttpAddr() == "" {
		return &gomrv1.RegisterWorkerResponse{
			Accepted: false,
			Message:  "http_addr is required",
		}, nil
	}
	if req.GetState() == gomrv1.WorkerState_WORKER_STATE_DEAD {
		return &gomrv1.RegisterWorkerResponse{
			Accepted: false,
			Message:  "worker cannot register as DEAD",
		}, nil
	}

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := probeWorkerHTTP(probeCtx, s.master.httpClient, req.GetHttpAddr()); err != nil {
		return &gomrv1.RegisterWorkerResponse{
			Accepted: false,
			Message:  fmt.Sprintf("worker health probe failed: %v", err),
		}, nil
	}

	now := time.Now()

	s.master.mu.Lock()
	s.master.workers[req.GetWorkerId()] = &Worker{
		ID:            req.GetWorkerId(),
		HTTPAddr:      req.GetHttpAddr(),
		State:         req.GetState(),
		RegisteredAt:  now,
		LastHeartbeat: now,
	}
	s.master.mu.Unlock()

	slog.Info("registered worker",
		"worker_id", req.GetWorkerId(),
		"addr", req.GetHttpAddr(),
		"state", req.GetState().String(),
	)

	return &gomrv1.RegisterWorkerResponse{
		Accepted:                 true,
		Message:                  "worker registered",
		HeartbeatIntervalSeconds: uint32(s.master.heartbeatInterval / time.Second),
		WorkerTimeoutSeconds:     uint32(s.master.workerTimeout / time.Second),
	}, nil
}

func (s *controlServer) Heartbeat(ctx context.Context, req *gomrv1.HeartbeatRequest) (*gomrv1.HeartbeatResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "worker_id is required")
	}

	s.master.mu.Lock()
	defer s.master.mu.Unlock()

	worker, ok := s.master.workers[req.GetWorkerId()]
	if !ok {
		return nil, grpcstatus.Errorf(codes.NotFound, "worker %s is not registered", req.GetWorkerId())
	}

	worker.State = req.GetState()
	worker.LastHeartbeat = time.Now()
	worker.CurrentTask = req.GetCurrentTask()

	resp := &gomrv1.HeartbeatResponse{}

	// 1. Process task result if the worker is reporting one.
	if result := req.GetLastResult(); result != nil {
		s.master.processResult(req.GetWorkerId(), result)
	}

	// 2. If worker is idle, try to assign a new task.
	if req.GetState() == gomrv1.WorkerState_WORKER_STATE_IDLE {
		resp.Assignment = s.master.assignTask(req.GetWorkerId())
	}

	// 3. If worker is busy on an aborted job, tell it to stop.
	if task := req.GetCurrentTask(); task != nil {
		if job, exists := s.master.jobs[task.JobId]; exists && job.Status == JobStatusAborted {
			resp.ShouldAbortCurrentTask = true
		}
	}

	return resp, nil
}

func probeWorkerHTTP(ctx context.Context, client *http.Client, httpAddr string) error {
	probeURL := httpAddr
	if !strings.HasPrefix(probeURL, "http://") && !strings.HasPrefix(probeURL, "https://") {
		probeURL = "http://" + probeURL
	}
	probeURL = strings.TrimRight(probeURL, "/") + "/health"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	return nil
}
