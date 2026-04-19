package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/sophic00/gomr/internal/config"
	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultHeartbeatInterval = 5 * time.Second

// Start initializes and runs the Gomr Worker on the given port,
// connecting to the Master gRPC endpoint.
func Start(cfg *config.Config) error {
	w := &Worker{
		ID:             uuid.NewString(),
		MasterGRPCAddr: cfg.MasterGRPCAddr,
		HTTPAddr:       net.JoinHostPort(cfg.WorkerHost, fmt.Sprintf("%d", cfg.WorkerPort)),
		State:          gomrv1.WorkerState_WORKER_STATE_IDLE,
	}

	listenAddr := fmt.Sprintf(":%d", cfg.WorkerPort)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(httpW http.ResponseWriter, r *http.Request) {
		httpW.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(httpW, "ok")
	})
	// placeholder: will be used for serving intermediate result
	mux.HandleFunc("/", func(httpW http.ResponseWriter, r *http.Request) {
		httpW.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(httpW, "Worker HTTP Data Plane on %s\n", w.HTTPAddr)
	})

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	serverErrCh := make(chan error, 1)
	httpServer := &http.Server{Handler: mux}
	go func() {
		log.Printf("Worker HTTP server listening on %s (advertised as %s)", listenAddr, w.HTTPAddr)
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
		close(serverErrCh)
	}()

	conn, err := grpc.NewClient(w.MasterGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to master gRPC endpoint %s: %w", w.MasterGRPCAddr, err)
	}
	defer conn.Close()

	client := gomrv1.NewMasterServiceClient(conn)
	heartbeatInterval, err := registerWorker(client, w)
	if err != nil {
		_ = httpServer.Shutdown(context.Background())
		return err
	}

	go startHeartbeatLoop(client, w, heartbeatInterval)

	if err, ok := <-serverErrCh; ok && err != nil {
		return fmt.Errorf("worker HTTP server failed: %w", err)
	}

	return nil
}

func registerWorker(client gomrv1.MasterServiceClient, w *Worker) (time.Duration, error) {
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

	log.Printf("Registered worker %s with master; heartbeat interval=%ds timeout=%ds", w.ID, resp.GetHeartbeatIntervalSeconds(), resp.GetWorkerTimeoutSeconds())
	if secs := resp.GetHeartbeatIntervalSeconds(); secs > 0 {
		return time.Duration(secs) * time.Second, nil
	}
	return defaultHeartbeatInterval, nil
}

func startHeartbeatLoop(client gomrv1.MasterServiceClient, w *Worker, interval time.Duration) {
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		w.mu.RLock()
		req := &gomrv1.HeartbeatRequest{
			WorkerId:    w.ID,
			State:       w.State,
			CurrentTask: w.CurrentTask,
		}
		w.mu.RUnlock()

		_, err := client.Heartbeat(ctx, req)
		cancel()
		if err != nil {
			log.Printf("Failed to send heartbeat for worker %s: %v", w.ID, err)
			continue
		}
		log.Printf("Sent heartbeat for worker %s (state: %s)", w.ID, req.State.String())
	}
}
