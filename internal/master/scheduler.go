package master

import (
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
)

// assignTask finds the next task to assign to an idle worker.
// Must be called with m.mu held. Returns nil if no work is available.
func (m *Master) assignTask(workerID string) *gomrv1.Assignment {
	// Ensure we have an active job.
	if m.activeJobID == "" {
		m.activateNextJob()
	}
	if m.activeJobID == "" {
		return nil // No jobs in queue.
	}

	job := m.jobs[m.activeJobID]

	switch job.Status {
	case JobStatusMapping:
		if a := m.assignMapTask(job, workerID); a != nil {
			return a
		}
		// If no map task could be assigned, check if we can assign a reduce task early
		if m.anyMapTaskCompleted(job) {
			return m.assignReduceTask(job, workerID)
		}
		return nil
	case JobStatusReducing:
		// First check for pending promotions (completed reduce tasks with temp objects).
		if a := m.assignPromotionTask(job, workerID); a != nil {
			return a
		}
		return m.assignReduceTask(job, workerID)
	default:
		// Active job is completed, aborted, or failed.
		// It should have been cleared elsewhere, but clear it here to be safe
		// so the next job can start.
		m.activeJobID = ""
		return nil
	}
}

// assignMapTask finds an idle map task and assigns it to the worker.
func (m *Master) assignMapTask(job *Job, workerID string) *gomrv1.Assignment {
	for _, mt := range job.MapTasks {
		if mt.Status != TaskStatusIdle {
			continue
		}

		attemptID := uuid.NewString()
		mt.Status = TaskStatusInProgress
		mt.Attempts = append(mt.Attempts, &TaskAttempt{
			AttemptID: attemptID,
			WorkerID:  workerID,
			StartedAt: time.Now(),
		})

		slog.Info("assigning map task",
			"job_id", job.ID,
			"task_id", mt.ID,
			"attempt_id", attemptID,
			"worker_id", workerID,
			"input_uri", mt.InputURI,
		)

		return &gomrv1.Assignment{
			Kind: &gomrv1.Assignment_Map{
				Map: &gomrv1.MapAssignment{
					JobId:            job.ID,
					TaskId:           mt.ID,
					InputUri:         mt.InputURI,
					MapSourceUri:     job.MapSourceURI,
					MapCompileCmd:    job.MapCompileCmd,
					ReducePartitions: uint32(job.NumReduceTasks),
					AttemptId:        attemptID,
				},
			},
		}
	}

	// No idle map tasks. Check for speculation.
	if len(job.MapCompletionTimes) >= 2 {
		median := medianDuration(job.MapCompletionTimes)
		threshold := time.Duration(float64(median) * 1.5)
		if threshold < 5*time.Second {
			threshold = 5 * time.Second
		}

		for _, mt := range job.MapTasks {
			if mt.Status != TaskStatusInProgress || len(mt.Attempts) >= 2 {
				continue
			}

			if elapsedSince(mt.Attempts[0].StartedAt) > threshold {
				attemptID := uuid.NewString()
				mt.Attempts = append(mt.Attempts, &TaskAttempt{
					AttemptID: attemptID,
					WorkerID:  workerID,
					StartedAt: time.Now(),
				})

				slog.Info("speculatively assigning map task",
					"job_id", job.ID,
					"task_id", mt.ID,
					"attempt_id", attemptID,
					"worker_id", workerID,
					"median_duration", median,
				)

				return &gomrv1.Assignment{
					Kind: &gomrv1.Assignment_Map{
						Map: &gomrv1.MapAssignment{
							JobId:            job.ID,
							TaskId:           mt.ID,
							InputUri:         mt.InputURI,
							MapSourceUri:     job.MapSourceURI,
							MapCompileCmd:    job.MapCompileCmd,
							ReducePartitions: uint32(job.NumReduceTasks),
						},
					},
				}
			}
		}
	}

	// No speculation possible. Check if all maps are done to advance phase.
	m.advanceJob(job)
	// After advancing, we might now be in reduce phase. Try to assign.
	if job.Status == JobStatusReducing {
		return m.assignReduceTask(job, workerID)
	}

	return nil
}

// assignReduceTask finds an idle reduce task and assigns it to the worker.
func (m *Master) assignReduceTask(job *Job, workerID string) *gomrv1.Assignment {
	for _, rt := range job.ReduceTasks {
		if rt.Status != TaskStatusIdle {
			continue
		}

		attemptID := uuid.NewString()
		rt.Status = TaskStatusInProgress
		rt.Attempts = append(rt.Attempts, &TaskAttempt{
			AttemptID: attemptID,
			WorkerID:  workerID,
			StartedAt: time.Now(),
		})

		// Collect partition URLs from all completed map tasks for this partition index.
		inputURLs := m.collectPartitionURLs(job, rt.Partition)

		slog.Info("assigning reduce task",
			"job_id", job.ID,
			"task_id", rt.ID,
			"attempt_id", attemptID,
			"worker_id", workerID,
			"partition", rt.Partition,
			"input_urls", len(inputURLs),
		)

		return &gomrv1.Assignment{
			Kind: &gomrv1.Assignment_Reduce{
				Reduce: &gomrv1.ReduceAssignment{
					JobId:            job.ID,
					TaskId:           rt.ID,
					Partition:        uint32(rt.Partition),
					ReduceSourceUri:  job.ReduceSourceURI,
					ReduceCompileCmd: job.ReduceCompileCmd,
					InputUrls:        inputURLs,
					OutputPrefix:     job.OutputPrefix,
					AllMapsComplete:  job.Status == JobStatusReducing,
					AttemptId:        attemptID,
				},
			},
		}
	}

	// No idle reduce tasks. Check for speculation.
	if len(job.ReduceCompletionTimes) >= 2 {
		median := medianDuration(job.ReduceCompletionTimes)
		threshold := time.Duration(float64(median) * 1.5)
		if threshold < 5*time.Second {
			threshold = 5 * time.Second
		}

		for _, rt := range job.ReduceTasks {
			if rt.Status != TaskStatusInProgress || len(rt.Attempts) >= 2 {
				continue
			}

			if elapsedSince(rt.Attempts[0].StartedAt) > threshold {
				attemptID := uuid.NewString()
				rt.Attempts = append(rt.Attempts, &TaskAttempt{
					AttemptID: attemptID,
					WorkerID:  workerID,
					StartedAt: time.Now(),
				})

				inputURLs := m.collectPartitionURLs(job, rt.Partition)

				slog.Info("speculatively assigning reduce task",
					"job_id", job.ID,
					"task_id", rt.ID,
					"attempt_id", attemptID,
					"worker_id", workerID,
					"partition", rt.Partition,
					"median_duration", median,
				)

				return &gomrv1.Assignment{
					Kind: &gomrv1.Assignment_Reduce{
						Reduce: &gomrv1.ReduceAssignment{
							JobId:            job.ID,
							TaskId:           rt.ID,
							Partition:        uint32(rt.Partition),
							ReduceSourceUri:  job.ReduceSourceURI,
							ReduceCompileCmd: job.ReduceCompileCmd,
							InputUrls:        inputURLs,
							OutputPrefix:     job.OutputPrefix,
							AllMapsComplete:  job.Status == JobStatusReducing,
							AttemptId:        attemptID,
						},
					},
				}
			}
		}
	}

	// No speculation possible. Check if all done.
	m.advanceJob(job)
	return nil
}

// assignPromotionTask finds a completed reduce task with a pending temp object
// and assigns a promotion task to move it to its final S3 location.
func (m *Master) assignPromotionTask(job *Job, workerID string) *gomrv1.Assignment {
	for _, rt := range job.ReduceTasks {
		if rt.Status != TaskStatusCompleted || rt.TempObject == "" {
			continue
		}

		// Build the final output object as a full S3 URI.
		bucket, prefix, _ := parseS3URI(job.OutputPrefix)
		finalObject := fmt.Sprintf("s3://%s/%spart-%d", bucket, prefix, rt.Partition)
		// TempObject is already stored as a full S3 URI by the reduce executor.
		attemptID := uuid.NewString()

		slog.Info("assigning promotion task",
			"job_id", job.ID,
			"task_id", rt.ID,
			"partition", rt.Partition,
			"worker_id", workerID,
			"temp_object", rt.TempObject,
			"final_object", finalObject,
		)

		return &gomrv1.Assignment{
			Kind: &gomrv1.Assignment_Promotion{
				Promotion: &gomrv1.PromotionAssignment{
					JobId:       job.ID,
					TaskId:      rt.ID,
					AttemptId:   attemptID,
					TempObject:  rt.TempObject,
					FinalObject: finalObject,
				},
			},
		}
	}
	return nil
}

func (m *Master) anyMapTaskCompleted(job *Job) bool {
	for _, mt := range job.MapTasks {
		if mt.Status == TaskStatusCompleted {
			return true
		}
	}
	return false
}

// collectPartitionURLs gathers the partition URL at index `partition` from every
// completed map task.
func (m *Master) collectPartitionURLs(job *Job, partition int) []string {
	var urls []string
	for _, mt := range job.MapTasks {
		if mt.Status != TaskStatusCompleted {
			continue
		}
		if partition < len(mt.PartitionURLs) {
			urls = append(urls, mt.PartitionURLs[partition])
		}
	}
	return urls
}

// processResult handles a task result reported by a worker via heartbeat.
// Must be called with m.mu held.
func (m *Master) processResult(workerID string, result *gomrv1.TaskResult) {
	if result == nil || result.Task == nil {
		return
	}
	ref := result.Task
	job, ok := m.jobs[ref.JobId]
	if !ok || job.Status == JobStatusAborted || job.Status == JobStatusCompleted {
		return
	}

	switch ref.Phase {
	case gomrv1.TaskPhase_TASK_PHASE_MAP:
		m.processMapResult(job, ref, result, workerID)
	case gomrv1.TaskPhase_TASK_PHASE_REDUCE:
		m.processReduceResult(job, ref, result, workerID)
	case gomrv1.TaskPhase_TASK_PHASE_PROMOTION:
		m.processPromotionResult(job, ref, result, workerID)
	}
}

func (m *Master) processMapResult(job *Job, ref *gomrv1.TaskRef, result *gomrv1.TaskResult, workerID string) {
	mt, ok := job.MapTaskIndex[ref.TaskId]
	if !ok {
		slog.Warn("map result for unknown task", "job_id", ref.JobId, "task_id", ref.TaskId)
		return
	}

	switch result.State {
	case gomrv1.TaskResultState_TASK_RESULT_STATE_SUCCESS:
		if mt.Status == TaskStatusCompleted {
			// Already completed by another attempt (speculative). Ignore.
			slog.Debug("ignoring duplicate map completion", "task_id", ref.TaskId)
			return
		}
		mt.Status = TaskStatusCompleted
		mt.PartitionURLs = result.PartitionUrls

		// Record completion time for speculation thresholds.
		if attempt := findAttempt(mt.Attempts, ref.AttemptId); attempt != nil {
			elapsed := elapsedSince(attempt.StartedAt)
			job.MapCompletionTimes = append(job.MapCompletionTimes, elapsed)
		}

		slog.Info("map task completed",
			"job_id", job.ID,
			"task_id", mt.ID,
			"worker_id", workerID,
			"partitions", len(result.PartitionUrls),
		)
		m.advanceJob(job)

	case gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED:
		slog.Warn("map task failed",
			"job_id", job.ID,
			"task_id", mt.ID,
			"worker_id", workerID,
			"error", result.ErrorMessage,
		)
		mt.Attempts = removeAttempt(mt.Attempts, ref.AttemptId)
		if len(mt.Attempts) == 0 {
			slog.Info("no active attempts left, resetting map task to idle", "task_id", mt.ID)
			mt.Status = TaskStatusIdle
		}

	case gomrv1.TaskResultState_TASK_RESULT_STATE_ABORTED:
		// No-op: task was already reset by eviction or abort.
	}
}

func (m *Master) processReduceResult(job *Job, ref *gomrv1.TaskRef, result *gomrv1.TaskResult, workerID string) {
	rt, ok := job.ReduceTaskIndex[ref.TaskId]
	if !ok {
		slog.Warn("reduce result for unknown task", "job_id", ref.JobId, "task_id", ref.TaskId)
		return
	}

	switch result.State {
	case gomrv1.TaskResultState_TASK_RESULT_STATE_SUCCESS:
		if rt.Status == TaskStatusCompleted {
			slog.Debug("ignoring duplicate reduce completion", "task_id", ref.TaskId)
			return
		}
		rt.Status = TaskStatusCompleted
		rt.WinningAttemptID = ref.AttemptId
		rt.TempObject = result.OutputObject

		if attempt := findAttempt(rt.Attempts, ref.AttemptId); attempt != nil {
			elapsed := elapsedSince(attempt.StartedAt)
			job.ReduceCompletionTimes = append(job.ReduceCompletionTimes, elapsed)
		}

		slog.Info("reduce task completed",
			"job_id", job.ID,
			"task_id", rt.ID,
			"partition", rt.Partition,
			"worker_id", workerID,
			"temp_object", rt.TempObject,
		)
		m.advanceJob(job)

	case gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED:
		slog.Warn("reduce task failed",
			"job_id", job.ID,
			"task_id", rt.ID,
			"worker_id", workerID,
			"error", result.ErrorMessage,
		)
		rt.Attempts = removeAttempt(rt.Attempts, ref.AttemptId)
		if len(rt.Attempts) == 0 {
			slog.Info("no active attempts left, resetting reduce task to idle", "task_id", rt.ID)
			rt.Status = TaskStatusIdle
		}

	case gomrv1.TaskResultState_TASK_RESULT_STATE_ABORTED:
		// No-op.
	}
}

func (m *Master) processPromotionResult(job *Job, ref *gomrv1.TaskRef, result *gomrv1.TaskResult, workerID string) {
	// Promotion is a lightweight operation. On success, mark the corresponding
	// reduce task as fully promoted. On failure, the reduce task can be retried.
	rt, ok := job.ReduceTaskIndex[ref.TaskId]
	if !ok {
		return
	}

	switch result.State {
	case gomrv1.TaskResultState_TASK_RESULT_STATE_SUCCESS:
		slog.Info("promotion completed",
			"job_id", job.ID,
			"task_id", rt.ID,
			"partition", rt.Partition,
		)
		// TempObject already promoted to final. Clear it.
		rt.TempObject = ""
		m.advanceJob(job)

	case gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED:
		slog.Warn("promotion failed",
			"job_id", job.ID,
			"task_id", rt.ID,
			"error", result.ErrorMessage,
		)
		// Promotion can be retried — the temp object is still in S3.
	}
}

// advanceJob checks if the current phase is complete and transitions the job.
// Must be called with m.mu held.
func (m *Master) advanceJob(job *Job) {
	switch job.Status {
	case JobStatusMapping:
		// Check if all map tasks are complete.
		for _, mt := range job.MapTasks {
			if mt.Status != TaskStatusCompleted {
				return
			}
		}
		job.Status = JobStatusReducing
		slog.Info("all map tasks complete, advancing to reduce phase", "job_id", job.ID)

	case JobStatusReducing:
		// Check if all reduce tasks are complete AND promoted (TempObject == "").
		for _, rt := range job.ReduceTasks {
			if rt.Status != TaskStatusCompleted || rt.TempObject != "" {
				return
			}
		}
		job.Status = JobStatusCompleted
		m.activeJobID = ""
		slog.Info("job completed", "job_id", job.ID)

	default:
		// Nothing to advance for Queued/Completed/Failed/Aborted.
	}
}

// activateNextJob pops the next job from the queue and sets it as active.
// Must be called with m.mu held.
func (m *Master) activateNextJob() {
	for {
		select {
		case jobID := <-m.queue:
			job, ok := m.jobs[jobID]
			if !ok {
				continue
			}
			// Skip aborted jobs.
			if job.Status == JobStatusAborted {
				slog.Info("skipping aborted job", "job_id", jobID)
				continue
			}
			job.Status = JobStatusMapping
			m.activeJobID = jobID
			slog.Info("activated job", "job_id", jobID,
				"map_tasks", len(job.MapTasks),
				"reduce_tasks", len(job.ReduceTasks),
			)
			return
		default:
			return // Queue is empty.
		}
	}
}

// --- Helpers ---

func findAttempt(attempts []*TaskAttempt, attemptID string) *TaskAttempt {
	for _, a := range attempts {
		if a.AttemptID == attemptID {
			return a
		}
	}
	return nil
}

// findAttemptByWorker returns the active attempt for a given worker, if any.
func findAttemptByWorker(attempts []*TaskAttempt, workerID string) *TaskAttempt {
	for _, a := range attempts {
		if a.WorkerID == workerID {
			return a
		}
	}
	return nil
}

// isTaskOwnedByWorker checks if any attempt of a task belongs to the given worker.
func isTaskOwnedByWorker(attempts []*TaskAttempt, workerID string) bool {
	return findAttemptByWorker(attempts, workerID) != nil
}

func elapsedSince(t time.Time) time.Duration {
	if t.IsZero() {
		return 0
	}
	return time.Since(t)
}

func medianDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func removeAttempt(attempts []*TaskAttempt, attemptID string) []*TaskAttempt {
	var kept []*TaskAttempt
	for _, a := range attempts {
		if a.AttemptID != attemptID {
			kept = append(kept, a)
		}
	}
	return kept
}

func removeAttemptByWorker(attempts []*TaskAttempt, workerID string) []*TaskAttempt {
	var kept []*TaskAttempt
	for _, a := range attempts {
		if a.WorkerID != workerID {
			kept = append(kept, a)
		}
	}
	return kept
}
