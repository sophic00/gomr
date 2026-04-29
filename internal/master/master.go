package master

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sophic00/gomr/internal/config"
	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
	"google.golang.org/grpc"
)

func NewMaster(cfg *config.Config) (*Master, error) {
	creds := credentials.NewEnvAWS()
	if cfg.AWSAccessKeyID != "" {
		creds = credentials.NewStaticV4(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, "")
	}

	minioClient, err := minio.New(cfg.S3Endpoint, &minio.Options{
		Creds:  creds,
		Secure: false,
		Region: cfg.AWSRegion,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize S3 client: %v", err)
	}

	return &Master{
		httpPort:          cfg.MasterHTTPPort,
		grpcPort:          cfg.MasterGRPCPort,
		jobs:              make(map[string]*Job),
		workers:           make(map[string]*Worker),
		queue:             make(chan string, 1000),
		s3Client:          minioClient,
		httpClient:        &http.Client{Timeout: 5 * time.Second},
		heartbeatInterval: 5 * time.Second,
		workerTimeout:     15 * time.Second,
	}, nil
}

// Start initializes and runs the Gomr Master daemon on the given port.
func Start(cfg *config.Config) error {
	m, err := NewMaster(cfg)
	if err != nil {
		return err
	}
	return m.Run()
}

func (m *Master) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpAddr := fmt.Sprintf(":%d", m.httpPort)
	grpcAddr := fmt.Sprintf(":%d", m.grpcPort)

	mux := m.setupRoutes()
	grpcServer := grpc.NewServer()
	gomrv1.RegisterMasterServiceServer(grpcServer, &controlServer{master: m})

	httpListener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen for master HTTP on %s: %w", httpAddr, err)
	}

	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		_ = httpListener.Close()
		return fmt.Errorf("failed to listen for master gRPC on %s: %w", grpcAddr, err)
	}

	httpServer := &http.Server{Handler: mux}
	errCh := make(chan error, 2)

	go func() {
		slog.Info("master HTTP API listening", "addr", httpAddr)
		errCh <- httpServer.Serve(httpListener)
	}()

	go func() {
		slog.Info("master gRPC control plane listening", "addr", grpcAddr)
		errCh <- grpcServer.Serve(grpcListener)
	}()

	go m.monitorWorkers(ctx)

	// Wait for shutdown signal or server error.
	select {
	case <-ctx.Done():
		slog.Info("received shutdown signal, draining...")
	case err = <-errCh:
		slog.Error("server error", "error", err)
	}

	// Graceful shutdown: give in-flight requests time to finish.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(shutdownCtx)

	return err
}

// monitorWorkers runs in the background and evicts workers that haven't sent a heartbeat within the timeout.
func (m *Master) monitorWorkers(ctx context.Context) {
	ticker := time.NewTicker(m.workerTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			now := time.Now()
			for workerID, worker := range m.workers {
				if now.Sub(worker.LastHeartbeat) > m.workerTimeout {
					slog.Warn("worker timed out, evicting",
						"worker_id", workerID,
						"last_heartbeat", worker.LastHeartbeat,
					)
					m.resetWorkerTasksLocked(workerID)
					delete(m.workers, workerID)
				}
			}
			m.mu.Unlock()
		}
	}
}

// resetWorkerTasksLocked resets any tasks that were assigned to a worker that is now considered dead.
// It removes the dead worker's attempt and only resets the task to Idle if no other attempts remain.
// It assumes m.mu is already locked for writing.
func (m *Master) resetWorkerTasksLocked(workerID string) {
	worker, exists := m.workers[workerID]
	if !exists || worker.CurrentTask == nil {
		return
	}

	taskRef := worker.CurrentTask
	job, exists := m.jobs[taskRef.JobId]
	if !exists || job.Status == JobStatusCompleted || job.Status == JobStatusAborted {
		return
	}

	switch taskRef.Phase {
	case gomrv1.TaskPhase_TASK_PHASE_MAP:
		if mt, ok := job.MapTaskIndex[taskRef.TaskId]; ok {
			if mt.Status == TaskStatusInProgress && isTaskOwnedByWorker(mt.Attempts, workerID) {
				mt.Attempts = removeAttemptByWorker(mt.Attempts, workerID)
				if len(mt.Attempts) == 0 {
					slog.Info("resetting map task from dead worker (no attempts remain)",
						"task_id", mt.ID, "job_id", job.ID, "worker_id", workerID,
					)
					mt.Status = TaskStatusIdle
				} else {
					slog.Info("removed dead worker attempt from map task, other attempt(s) still active",
						"task_id", mt.ID, "job_id", job.ID, "worker_id", workerID,
						"remaining_attempts", len(mt.Attempts),
					)
				}
			}
		}
	case gomrv1.TaskPhase_TASK_PHASE_REDUCE:
		if rt, ok := job.ReduceTaskIndex[taskRef.TaskId]; ok {
			if rt.Status == TaskStatusInProgress && isTaskOwnedByWorker(rt.Attempts, workerID) {
				rt.Attempts = removeAttemptByWorker(rt.Attempts, workerID)
				if len(rt.Attempts) == 0 {
					slog.Info("resetting reduce task from dead worker (no attempts remain)",
						"task_id", rt.ID, "job_id", job.ID, "worker_id", workerID,
					)
					rt.Status = TaskStatusIdle
				} else {
					slog.Info("removed dead worker attempt from reduce task, other attempt(s) still active",
						"task_id", rt.ID, "job_id", job.ID, "worker_id", workerID,
						"remaining_attempts", len(rt.Attempts),
					)
				}
			}
		}
	}
}

