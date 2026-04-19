package master

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
)

// handleSubmit handles new MapReduce job submissions.
func (m *Master) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var sub JobSubmission
	if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Basic validation
	if sub.PluginURI == "" || sub.InputPrefix == "" || sub.OutputPrefix == "" || sub.ReduceTasks <= 0 {
		http.Error(w, "Missing required fields or invalid reduce_tasks count", http.StatusBadRequest)
		return
	}

	bucket, prefix, err := parseS3URI(sub.InputPrefix)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid input_prefix URI: %v", err), http.StatusBadRequest)
		return
	}

	// 1. List objects to calculate M (Map Tasks)
	ctx := r.Context()
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	var mapTasks []*MapTask
	objectCh := m.s3Client.ListObjects(ctx, bucket, opts)
	for object := range objectCh {
		if object.Err != nil {
			log.Printf("Error listing S3 objects: %v", object.Err)
			http.Error(w, "Failed to list input files", http.StatusInternalServerError)
			return
		}
		// Skip directories (if any somehow match)
		if strings.HasSuffix(object.Key, "/") {
			continue
		}

		mapTasks = append(mapTasks, &MapTask{
			ID:       uuid.New().String(),
			InputURI: fmt.Sprintf("s3://%s/%s", bucket, object.Key),
			Status:   TaskStatusIdle,
		})
	}

	if len(mapTasks) == 0 {
		http.Error(w, "No input files found at the specified prefix", http.StatusBadRequest)
		return
	}

	// 2. Generate Reduce Tasks
	var reduceTasks []*ReduceTask
	for i := 0; i < sub.ReduceTasks; i++ {
		reduceTasks = append(reduceTasks, &ReduceTask{
			ID:     uuid.New().String(),
			Status: TaskStatusIdle,
		})
	}

	// 3. Create the Job
	jobID := "job-" + uuid.New().String()
	job := &Job{
		ID:             jobID,
		PluginURI:      sub.PluginURI,
		InputPrefix:    sub.InputPrefix,
		OutputPrefix:   sub.OutputPrefix,
		NumReduceTasks: sub.ReduceTasks,
		Status:         JobStatusQueued,
		MapTasks:       mapTasks,
		ReduceTasks:    reduceTasks,
	}

	// 4. Register and Enqueue
	m.mu.Lock()
	m.jobs[jobID] = job
	m.mu.Unlock()

	// Push to job queue (non-blocking since it's buffered)
	m.queue <- jobID

	log.Printf("Job %s submitted: %d Map tasks, %d Reduce tasks\n", jobID, len(mapTasks), sub.ReduceTasks)

	// 5. Send Response
	resp := JobSubmitResponse{
		JobID:            jobID,
		Status:           "Accepted",
		Message:          "Job successfully submitted and queued.",
		MapTasksCount:    len(mapTasks),
		ReduceTasksCount: sub.ReduceTasks,
		QueueSize:        len(m.queue),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(resp)
}

// handleStatus retrieves progress of all jobs and worker statistics
func (m *Master) handleStatus(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	workerStats := make(map[string]int)
	for _, w := range m.workers {
		stateStr := w.State.String()
		workerStats[stateStr]++
	}

	var jobStatuses []JobStatusInfo
	for _, job := range m.jobs {
		mapCompleted := 0
		for _, mt := range job.MapTasks {
			if mt.Status == TaskStatusCompleted {
				mapCompleted++
			}
		}

		reduceCompleted := 0
		for _, rt := range job.ReduceTasks {
			if rt.Status == TaskStatusCompleted {
				reduceCompleted++
			}
		}

		jobStatuses = append(jobStatuses, JobStatusInfo{
			JobID:          job.ID,
			Status:         job.Status,
			MapProgress:    fmt.Sprintf("%d/%d", mapCompleted, len(job.MapTasks)),
			ReduceProgress: fmt.Sprintf("%d/%d", reduceCompleted, len(job.ReduceTasks)),
		})
	}

	resp := SystemStatusResponse{
		Workers: workerStats,
		Jobs:    jobStatuses,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleDelete cancels a specific job and cleans up its data
func (m *Master) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "Missing job id", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	job, exists := m.jobs[id]
	if !exists {
		m.mu.Unlock()
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	if job.Status == JobStatusCompleted || job.Status == JobStatusAborted {
		m.mu.Unlock()
		http.Error(w, "Job already finished or aborted", http.StatusBadRequest)
		return
	}

	job.Status = JobStatusAborted

	// Note: We don't remove from `m.queue` directly because channel removal is complex.
	// Instead, the scheduler will pop the job, see it's "Aborted", and simply skip it.
	m.mu.Unlock()

	// Clean up S3 in the background to not block the HTTP response
	go func(j *Job) {
		log.Printf("Cleaning up S3 for aborted job %s...", j.ID)
		ctx := context.Background()

		// Attempt to delete final output prefix from S3
		if err := deleteS3Prefix(ctx, m.s3Client, j.OutputPrefix); err != nil {
			log.Printf("Failed to clean up output for job %s: %v", j.ID, err)
		} else {
			log.Printf("Cleanup finished for job %s", j.ID)
		}
	}(job)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Job %s aborted and cleanup initiated\n", id)
}
