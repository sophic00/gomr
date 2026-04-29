package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sophic00/gomr/internal/config"
	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultHeartbeatInterval = 5 * time.Second

// NewWorker creates a new Worker instance from the given config.
func NewWorker(cfg *config.Config) (*Worker, error) {
	creds := credentials.NewEnvAWS()
	if cfg.AWSAccessKeyID != "" {
		creds = credentials.NewStaticV4(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, "")
	}

	s3Client, err := minio.New(cfg.S3Endpoint, &minio.Options{
		Creds:  creds,
		Secure: false,
		Region: cfg.AWSRegion,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize S3 client: %w", err)
	}

	id := uuid.NewString()
	workDir := filepath.Join(os.TempDir(), "gomr-worker-"+id)
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create work directory: %w", err)
	}

	return &Worker{
		ID:             id,
		MasterGRPCAddr: cfg.MasterGRPCAddr,
		HTTPAddr:       net.JoinHostPort(cfg.WorkerHost, fmt.Sprintf("%d", cfg.WorkerHTTPPort)),
		State:          gomrv1.WorkerState_WORKER_STATE_IDLE,
		s3Client:       s3Client,
		workDir:        workDir,
		spillThreshold: cfg.IntermediateSpillThreshold * 1024 * 1024, // MB → bytes
		reduceUpdates:  make(chan *gomrv1.HeartbeatResponse, 5),
	}, nil
}

// Start initializes and runs the Gomr Worker, connecting to the Master gRPC endpoint.
func Start(cfg *config.Config) error {
	w, err := NewWorker(cfg)
	if err != nil {
		return err
	}
	return w.Run(cfg.WorkerHTTPPort)
}

// Run starts the worker's HTTP server, registers with the master, and begins the heartbeat loop.
func (w *Worker) Run(port int) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	defer os.RemoveAll(w.workDir)

	listenAddr := fmt.Sprintf(":%d", port)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", w.handleHealth)
	mux.HandleFunc("/partitions/", w.handlePartition)
	mux.HandleFunc("/", w.handleRoot)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	httpServer := &http.Server{Handler: mux}
	serverErrCh := make(chan error, 1)
	go func() {
		slog.Info("worker HTTP server listening", "listen_addr", listenAddr, "advertised_addr", w.HTTPAddr)
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
		close(serverErrCh)
	}()

	conn, err := grpc.NewClient(w.MasterGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = httpServer.Shutdown(context.Background())
		return fmt.Errorf("failed to connect to master gRPC endpoint %s: %w", w.MasterGRPCAddr, err)
	}
	defer conn.Close()

	client := gomrv1.NewMasterServiceClient(conn)
	heartbeatInterval, err := w.register(client)
	if err != nil {
		_ = httpServer.Shutdown(context.Background())
		return err
	}

	go w.heartbeatLoop(ctx, client, heartbeatInterval)

	// Wait for shutdown signal or server error.
	select {
	case <-ctx.Done():
		slog.Info("received shutdown signal, draining...", "worker_id", w.ID)
	case err = <-serverErrCh:
		if err != nil {
			return fmt.Errorf("worker HTTP server failed: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)

	return nil
}

// --- HTTP Handlers ---

func (w *Worker) handleHealth(hw http.ResponseWriter, r *http.Request) {
	hw.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(hw, "ok")
}

func (w *Worker) handleRoot(hw http.ResponseWriter, r *http.Request) {
	hw.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(hw, "Worker HTTP Data Plane on %s\n", w.HTTPAddr)
}

// handlePartition serves partition data for reduce workers.
func (w *Worker) handlePartition(hw http.ResponseWriter, r *http.Request) {
	w.mu.RLock()
	ps := w.partitions
	w.mu.RUnlock()

	if ps == nil {
		http.Error(hw, "no partitions available", http.StatusNotFound)
		return
	}
	ps.ServePartition(hw, r)
}

// --- gRPC Communication ---

func (w *Worker) register(client gomrv1.MasterServiceClient) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w.mu.RLock()
	req := &gomrv1.RegisterWorkerRequest{
		WorkerId: w.ID,
		HttpAddr: w.HTTPAddr,
		State:    w.State,
	}
	w.mu.RUnlock()

	resp, err := client.RegisterWorker(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("worker registration failed: %w", err)
	}
	if !resp.GetAccepted() {
		return 0, fmt.Errorf("worker registration rejected: %s", resp.GetMessage())
	}

	slog.Info("registered with master",
		"worker_id", w.ID,
		"heartbeat_interval_s", resp.GetHeartbeatIntervalSeconds(),
		"timeout_s", resp.GetWorkerTimeoutSeconds(),
	)
	if secs := resp.GetHeartbeatIntervalSeconds(); secs > 0 {
		return time.Duration(secs) * time.Second, nil
	}
	return defaultHeartbeatInterval, nil
}

func (w *Worker) heartbeatLoop(ctx context.Context, client gomrv1.MasterServiceClient, interval time.Duration) {
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

			w.mu.Lock()
			req := &gomrv1.HeartbeatRequest{
				WorkerId:    w.ID,
				State:       w.State,
				CurrentTask: w.CurrentTask,
				LastResult:  w.lastResult,
			}
			// Clear lastResult after including it in the heartbeat.
			w.lastResult = nil
			w.mu.Unlock()

			resp, err := client.Heartbeat(hbCtx, req)
			cancel()
			if err != nil {
				slog.Warn("heartbeat failed", "worker_id", w.ID, "error", err)
				if strings.Contains(err.Error(), "not registered") {
					slog.Info("worker not registered, attempting re-registration...", "worker_id", w.ID)
					newInterval, regErr := w.register(client)
					if regErr != nil {
						slog.Error("re-registration failed", "worker_id", w.ID, "error", regErr)
					} else {
						slog.Info("re-registration successful", "worker_id", w.ID)
						if newInterval != interval && newInterval > 0 {
							interval = newInterval
							ticker.Reset(interval)
						}
					}
				}
				continue
			}

			// Handle abort signal.
			if resp.GetShouldAbortCurrentTask() {
				w.abortCurrentTask()
			}

			// Handle new assignment (only if we're idle).
			if assignment := resp.GetAssignment(); assignment != nil {
				go w.handleAssignment(ctx, assignment)
			}

			// Handle early reduce prefetch updates.
			if len(resp.GetAdditionalReduceUrls()) > 0 || resp.GetAllMapsComplete() {
				select {
				case w.reduceUpdates <- resp:
				default:
					slog.Debug("reduceUpdates channel full or no listener, dropping update", "worker_id", w.ID)
				}
			}

			slog.Debug("heartbeat sent", "worker_id", w.ID, "state", req.State.String())
		}
	}
}

// --- Task Dispatch ---

// handleAssignment dispatches to the appropriate executor based on assignment type.
func (w *Worker) handleAssignment(ctx context.Context, assignment *gomrv1.Assignment) {
	// Create a cancellable context for this task.
	taskCtx, taskCancel := context.WithCancel(ctx)

	w.mu.Lock()
	w.cancelTask = taskCancel
	w.State = gomrv1.WorkerState_WORKER_STATE_BUSY
	w.mu.Unlock()

	var result *gomrv1.TaskResult

	switch kind := assignment.Kind.(type) {
	case *gomrv1.Assignment_Map:
		w.mu.Lock()
		w.CurrentTask = &gomrv1.TaskRef{
			JobId:     kind.Map.JobId,
			TaskId:    kind.Map.TaskId,
			Phase:     gomrv1.TaskPhase_TASK_PHASE_MAP,
			AttemptId: kind.Map.AttemptId,
		}
		w.mu.Unlock()

		result = w.executeMap(taskCtx, kind.Map)

	case *gomrv1.Assignment_Reduce:
		w.mu.Lock()
		w.CurrentTask = &gomrv1.TaskRef{
			JobId:     kind.Reduce.JobId,
			TaskId:    kind.Reduce.TaskId,
			Phase:     gomrv1.TaskPhase_TASK_PHASE_REDUCE,
			AttemptId: kind.Reduce.AttemptId,
		}
		w.mu.Unlock()

		result = w.executeReduce(taskCtx, kind.Reduce)

	case *gomrv1.Assignment_Promotion:
		w.mu.Lock()
		w.CurrentTask = &gomrv1.TaskRef{
			JobId:     kind.Promotion.JobId,
			TaskId:    kind.Promotion.TaskId,
			Phase:     gomrv1.TaskPhase_TASK_PHASE_PROMOTION,
			AttemptId: kind.Promotion.AttemptId,
		}
		w.mu.Unlock()

		result = w.executePromotion(taskCtx, kind.Promotion)
	}

	// Store result for the next heartbeat and transition back to idle.
	w.mu.Lock()
	w.lastResult = result
	w.State = gomrv1.WorkerState_WORKER_STATE_IDLE
	w.CurrentTask = nil
	w.cancelTask = nil
	w.mu.Unlock()

	taskCancel() // Clean up context.
}

// abortCurrentTask cancels the currently running task, if any.
func (w *Worker) abortCurrentTask() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.cancelTask != nil {
		slog.Info("aborting current task", "worker_id", w.ID)
		w.cancelTask()
	}
}
