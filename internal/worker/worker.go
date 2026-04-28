package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sophic00/gomr/internal/config"
	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultHeartbeatInterval = 5 * time.Second

// NewWorker creates a new Worker instance from the given config.
func NewWorker(cfg *config.Config) *Worker {
	return &Worker{
		ID:             uuid.NewString(),
		MasterGRPCAddr: cfg.MasterGRPCAddr,
		HTTPAddr:       net.JoinHostPort(cfg.WorkerHost, fmt.Sprintf("%d", cfg.WorkerHTTPPort)),
		State:          gomrv1.WorkerState_WORKER_STATE_IDLE,
	}
}

// Start initializes and runs the Gomr Worker, connecting to the Master gRPC endpoint.
func Start(cfg *config.Config) error {
	w := NewWorker(cfg)
	return w.Run(cfg.WorkerHTTPPort)
}

// Run starts the worker's HTTP server, registers with the master, and begins the heartbeat loop.
func (w *Worker) Run(port int) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	listenAddr := fmt.Sprintf(":%d", port)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", w.handleHealth)
	// Placeholder: will serve intermediate partition data for reduce workers.
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

// handleHealth responds to health check probes from the master.
func (w *Worker) handleHealth(hw http.ResponseWriter, r *http.Request) {
	hw.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(hw, "ok")
}

// handleRoot is a placeholder data plane endpoint.
func (w *Worker) handleRoot(hw http.ResponseWriter, r *http.Request) {
	hw.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(hw, "Worker HTTP Data Plane on %s\n", w.HTTPAddr)
}

// register sends a RegisterWorker RPC to the master and returns the negotiated heartbeat interval.
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

// heartbeatLoop sends periodic heartbeats to the master, re-registering if necessary.
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

			w.mu.RLock()
			req := &gomrv1.HeartbeatRequest{
				WorkerId:    w.ID,
				State:       w.State,
				CurrentTask: w.CurrentTask,
			}
			w.mu.RUnlock()

			_, err := client.Heartbeat(hbCtx, req)
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
			slog.Debug("heartbeat sent", "worker_id", w.ID, "state", req.State.String())
		}
	}
}
