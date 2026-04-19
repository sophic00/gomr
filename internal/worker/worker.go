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
	workerID := uuid.NewString()
	listenAddr := fmt.Sprintf(":%d", cfg.WorkerPort)
	advertisedAddr := net.JoinHostPort(cfg.WorkerHost, fmt.Sprintf("%d", cfg.WorkerPort))

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "ok")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "Worker HTTP Data Plane on %s\n", advertisedAddr)
	})

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	serverErrCh := make(chan error, 1)
	httpServer := &http.Server{Handler: mux}
	go func() {
		log.Printf("Worker HTTP server listening on %s (advertised as %s)", listenAddr, advertisedAddr)
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
		close(serverErrCh)
	}()

	conn, err := grpc.NewClient(cfg.MasterGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to master gRPC endpoint %s: %w", cfg.MasterGRPCAddr, err)
	}
	defer conn.Close()

	client := gomrv1.NewMasterServiceClient(conn)
	heartbeatInterval, err := registerWorker(client, workerID, advertisedAddr)
	if err != nil {
		_ = httpServer.Shutdown(context.Background())
		return err
	}

	go startHeartbeatLoop(client, workerID, heartbeatInterval)

	if err, ok := <-serverErrCh; ok && err != nil {
		return fmt.Errorf("worker HTTP server failed: %w", err)
	}

	return nil
}

func registerWorker(client gomrv1.MasterServiceClient, workerID, httpAddr string) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.RegisterWorker(ctx, &gomrv1.RegisterWorkerRequest{
		WorkerId: workerID,
		HttpAddr: httpAddr,
		State:    gomrv1.WorkerState_WORKER_STATE_IDLE,
	})
	if err != nil {
		return 0, fmt.Errorf("worker registration failed: %w", err)
	}
	if !resp.GetAccepted() {
		return 0, fmt.Errorf("worker registration rejected: %s", resp.GetMessage())
	}

	log.Printf("Registered worker %s with master; heartbeat interval=%ds timeout=%ds", workerID, resp.GetHeartbeatIntervalSeconds(), resp.GetWorkerTimeoutSeconds())
	if secs := resp.GetHeartbeatIntervalSeconds(); secs > 0 {
		return time.Duration(secs) * time.Second, nil
	}
	return defaultHeartbeatInterval, nil
}

func startHeartbeatLoop(client gomrv1.MasterServiceClient, workerID string, interval time.Duration) {
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Heartbeat(ctx, &gomrv1.HeartbeatRequest{
			WorkerId: workerID,
			State:    gomrv1.WorkerState_WORKER_STATE_IDLE,
		})
		cancel()
		if err != nil {
			log.Printf("Failed to send heartbeat for worker %s: %v", workerID, err)
			continue
		}
		log.Printf("Sent heartbeat for worker %s", workerID)
	}
}
