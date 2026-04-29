package master

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

const checkpointObjectKey = "latest.json"

// checkpointKey returns the full S3 object key for the checkpoint file.
func (m *Master) checkpointKey() string {
	_, prefix, _ := parseS3URI(m.checkpointS3URI)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix + checkpointObjectKey
}

// checkpointBucket returns the S3 bucket from the checkpoint URI.
func (m *Master) checkpointBucket() string {
	bucket, _, _ := parseS3URI(m.checkpointS3URI)
	return bucket
}

// saveCheckpoint serializes the master's job state to S3.
// Must NOT be called with m.mu held — it acquires the lock internally.
func (m *Master) saveCheckpoint(ctx context.Context) error {
	m.mu.RLock()
	cp := Checkpoint{
		ActiveJobID: m.activeJobID,
		Jobs:        m.jobs,
		SavedAt:     time.Now(),
	}
	data, err := json.Marshal(cp)
	m.mu.RUnlock()

	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	bucket := m.checkpointBucket()
	key := m.checkpointKey()

	_, err = m.s3Client.PutObject(ctx, bucket, key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint to s3://%s/%s: %w", bucket, key, err)
	}

	slog.Info("checkpoint saved",
		"bucket", bucket,
		"key", key,
		"jobs", len(cp.Jobs),
		"size_bytes", len(data),
	)
	return nil
}

// loadCheckpoint reads a checkpoint from S3 and restores master state.
// On restore:
//   - All InProgress tasks are reset to Idle (workers are gone after a restart).
//   - All Attempts are cleared (worker assignments are stale).
//   - MapTaskIndex and ReduceTaskIndex are rebuilt from the task slices.
//   - The job queue is reconstructed from jobs with Queued status.
//
// Must be called before any workers connect (during startup).
func (m *Master) loadCheckpoint(ctx context.Context) error {
	bucket := m.checkpointBucket()
	key := m.checkpointKey()

	obj, err := m.s3Client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get checkpoint object: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint: %w", err)
	}

	// Handle empty object or stat failure (e.g., minio returns 0 bytes for missing keys).
	if len(data) == 0 {
		slog.Info("checkpoint object is empty, starting fresh")
		return nil
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	// Restore state under the write lock.
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs = cp.Jobs
	m.activeJobID = cp.ActiveJobID

	// Post-restore fixup: rebuild indexes, reset in-progress tasks, re-enqueue queued jobs.
	for _, job := range m.jobs {
		// Rebuild index maps.
		job.MapTaskIndex = make(map[string]*MapTask, len(job.MapTasks))
		for _, mt := range job.MapTasks {
			job.MapTaskIndex[mt.ID] = mt

			// Reset in-progress tasks — workers are gone after a restart.
			if mt.Status == TaskStatusInProgress {
				mt.Status = TaskStatusIdle
				mt.Attempts = nil
			}
		}

		job.ReduceTaskIndex = make(map[string]*ReduceTask, len(job.ReduceTasks))
		for _, rt := range job.ReduceTasks {
			job.ReduceTaskIndex[rt.ID] = rt

			if rt.Status == TaskStatusInProgress {
				rt.Status = TaskStatusIdle
				rt.Attempts = nil
			}
		}

		// Re-enqueue queued jobs.
		if job.Status == JobStatusQueued {
			select {
			case m.queue <- job.ID:
			default:
				slog.Warn("queue full, could not re-enqueue job", "job_id", job.ID)
			}
		}
	}

	slog.Info("checkpoint restored",
		"saved_at", cp.SavedAt,
		"jobs", len(cp.Jobs),
		"active_job_id", cp.ActiveJobID,
	)
	return nil
}

// checkpointLoop periodically saves the master's state to S3.
func (m *Master) checkpointLoop(ctx context.Context) {
	ticker := time.NewTicker(m.checkpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final checkpoint on shutdown.
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := m.saveCheckpoint(shutdownCtx); err != nil {
				slog.Error("failed to save final checkpoint on shutdown", "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			if err := m.saveCheckpoint(ctx); err != nil {
				slog.Error("periodic checkpoint failed", "error", err)
			}
		}
	}
}
