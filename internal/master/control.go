package master

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
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
	if err := probeWorkerHTTP(probeCtx, req.GetHttpAddr()); err != nil {
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

	log.Printf("Registered worker %s at %s with state %s", req.GetWorkerId(), req.GetHttpAddr(), req.GetState().String())

	return &gomrv1.RegisterWorkerResponse{
		Accepted:                 true,
		Message:                  "worker registered",
		HeartbeatIntervalSeconds: uint32(s.master.heartbeatInterval / time.Second),
		WorkerTimeoutSeconds:     uint32(s.master.workerTimeout / time.Second),
	}, nil
}

func (s *controlServer) Heartbeat(ctx context.Context, req *gomrv1.HeartbeatRequest) (*gomrv1.HeartbeatResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, fmt.Errorf("worker_id is required")
	}

	s.master.mu.Lock()
	defer s.master.mu.Unlock()

	worker, ok := s.master.workers[req.GetWorkerId()]
	if !ok {
		return nil, fmt.Errorf("worker %s is not registered", req.GetWorkerId())
	}

	worker.State = req.GetState()
	worker.LastHeartbeat = time.Now()

	return &gomrv1.HeartbeatResponse{}, nil
}

func probeWorkerHTTP(ctx context.Context, httpAddr string) error {
	probeURL := httpAddr
	if !strings.HasPrefix(probeURL, "http://") && !strings.HasPrefix(probeURL, "https://") {
		probeURL = "http://" + probeURL
	}
	probeURL = strings.TrimRight(probeURL, "/") + "/health"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 3 * time.Second}
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
