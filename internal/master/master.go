package master

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func NewMaster(port int) (*Master, error) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "thia:3900"
	}

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize S3 client: %v", err)
	}

	return &Master{
		port:     port,
		jobs:     make(map[string]*Job),
		queue:    make(chan string, 1000),
		s3Client: minioClient,
	}, nil
}

// Start initializes and runs the Gomr Master daemon on the given port.
func Start(port int) error {
	m, err := NewMaster(port)
	if err != nil {
		return err
	}
	return m.Run()
}

func (m *Master) Run() error {
	addr := fmt.Sprintf(":%d", m.port)

	http.HandleFunc("/submit", m.handleSubmit)

	log.Printf("Master listening on %s\n", addr)
	return http.ListenAndServe(addr, nil)
}

func parseS3URI(uri string) (bucket, prefix string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("invalid scheme: %s (expected s3)", u.Scheme)
	}
	bucket = u.Host
	prefix = strings.TrimPrefix(u.Path, "/")
	return bucket, prefix, nil
}

func (m *Master) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

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
