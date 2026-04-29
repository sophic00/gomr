package worker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	gomrv1 "github.com/sophic00/gomr/proto/gomr/v1"
)

// executeMap runs a map task:
//  1. Download and compile the map source.
//  2. Stream S3 input to the child process stdin.
//  3. Read key\tvalue lines from stdout and partition them.
//  4. Return partition URLs on success.
func (w *Worker) executeMap(ctx context.Context, a *gomrv1.MapAssignment) *gomrv1.TaskResult {
	result := &gomrv1.TaskResult{
		Task: &gomrv1.TaskRef{
			JobId:     a.JobId,
			TaskId:    a.TaskId,
			Phase:     gomrv1.TaskPhase_TASK_PHASE_MAP,
			AttemptId: a.AttemptId,
		},
	}

	// 1. Download and compile map source.
	binaryPath, err := w.downloadAndCompile(ctx, a.MapSourceUri, a.MapCompileCmd)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("compile failed: %v", err)
		slog.Error("map compile failed", "job_id", a.JobId, "task_id", a.TaskId, "error", err)
		return result
	}

	// 2. Open S3 input object as stream.
	bucket, key, err := parseS3URI(a.InputUri)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("invalid input URI: %v", err)
		return result
	}

	inputObj, err := w.s3Client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("failed to get input object: %v", err)
		return result
	}
	defer inputObj.Close()

	childCtx, cancelChild := context.WithCancel(ctx)
	defer cancelChild()

	// 3. Run child process: stdin=S3 stream, capture stdout.
	stdout, wait, err := runChildProcess(childCtx, binaryPath, inputObj)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("failed to start child process: %v", err)
		return result
	}

	// 4. Create partition store and read child output.
	ps := NewPartitionStore(
		int(a.ReducePartitions),
		w.spillThreshold,
		a.JobId,
		a.TaskId,
		w.workDir,
	)

	var mapError error
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if err := ps.Write(scanner.Bytes()); err != nil {
			mapError = fmt.Errorf("failed to write map output partition: %w", err)
			break
		}
	}
	if mapError == nil {
		if err := scanner.Err(); err != nil {
			mapError = fmt.Errorf("failed to read map child stdout: %w", err)
		}
	}

	if mapError != nil {
		cancelChild()
		_ = wait()
		ps.Cleanup()
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = mapError.Error()
		slog.Warn("map task output processing failed",
			"job_id", a.JobId,
			"task_id", a.TaskId,
			"error", mapError,
		)
		return result
	}

	// Wait for child to exit.
	if err := wait(); err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("map child process failed: %v", err)
		ps.Cleanup()
		return result
	}

	// 5. Store partitions for HTTP serving and build URLs.
	w.mu.Lock()
	w.partitions = ps
	w.mu.Unlock()

	partitionURLs := ps.URLs(w.HTTPAddr)

	result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_SUCCESS
	result.PartitionUrls = partitionURLs

	slog.Info("map task completed",
		"job_id", a.JobId,
		"task_id", a.TaskId,
		"partitions", len(partitionURLs),
	)

	return result
}

// executeReduce runs a reduce task:
//  1. Download and compile the reduce source.
//  2. Download all partition files from map workers via HTTP.
//  3. Sort all data by key.
//  4. Pipe sorted data to the reduce child process.
//  5. Upload child output to S3 as a temporary object.
func (w *Worker) executeReduce(ctx context.Context, a *gomrv1.ReduceAssignment) *gomrv1.TaskResult {
	result := &gomrv1.TaskResult{
		Task: &gomrv1.TaskRef{
			JobId:     a.JobId,
			TaskId:    a.TaskId,
			Phase:     gomrv1.TaskPhase_TASK_PHASE_REDUCE,
			AttemptId: a.AttemptId,
		},
	}

	// 1. Download and compile reduce source.
	binaryPath, err := w.downloadAndCompile(ctx, a.ReduceSourceUri, a.ReduceCompileCmd)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("compile failed: %v", err)
		slog.Error("reduce compile failed", "job_id", a.JobId, "task_id", a.TaskId, "error", err)
		return result
	}

	// Drain any stale updates.
	for {
		select {
		case <-w.reduceUpdates:
		default:
			goto Drained
		}
	}
Drained:

	// 2. Download partition files incrementally (Early Reduce Prefetch).
	slog.Info("gathering partitions for reduce",
		"job_id", a.JobId,
		"task_id", a.TaskId,
		"partition", a.Partition,
	)

	var allLines [][]byte
	httpClient := &http.Client{Timeout: 60 * time.Second}

	downloadedURLs := make(map[string]bool)
	pendingURLs := make(map[string]bool)
	for _, u := range a.InputUrls {
		pendingURLs[u] = true
	}
	allMapsComplete := a.AllMapsComplete

	pollTimer := time.NewTimer(1 * time.Second)
	defer pollTimer.Stop()

	for {
		// Download any pending URLs
		for u := range pendingURLs {
			lines, err := downloadPartition(ctx, httpClient, u)
			if err != nil {
				result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
				result.ErrorMessage = fmt.Sprintf("failed to download partition from %s: %v", u, err)
				slog.Error("partition download failed", "url", u, "error", err)
				return result
			}
			allLines = append(allLines, lines...)
			downloadedURLs[u] = true
			delete(pendingURLs, u)
		}

		if allMapsComplete && len(pendingURLs) == 0 {
			break
		}

		// Wait for updates or tick
		pollTimer.Reset(1 * time.Second)
		select {
		case <-ctx.Done():
			result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
			result.ErrorMessage = "context canceled"
			return result
		case update := <-w.reduceUpdates:
			if update.AllMapsComplete {
				allMapsComplete = true
			}
			for _, u := range update.AdditionalReduceUrls {
				if !downloadedURLs[u] {
					pendingURLs[u] = true
				}
			}
		case <-pollTimer.C:
			// Keep polling periodically, though channel should wake us
		}
	}
	// 3. Sort all lines by key (everything before the first tab).
	sort.Slice(allLines, func(i, j int) bool {
		keyI := extractKey(allLines[i])
		keyJ := extractKey(allLines[j])
		return bytes.Compare(keyI, keyJ) < 0
	})

	// Build a reader from sorted lines.
	sortedData := bytes.Join(allLines, []byte{'\n'})
	if len(sortedData) > 0 {
		sortedData = append(sortedData, '\n')
	}
	sortedReader := bytes.NewReader(sortedData)

	// 4. Run reduce child process.
	stdout, wait, err := runChildProcess(ctx, binaryPath, sortedReader)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("failed to start reduce child: %v", err)
		return result
	}

	// Read all child output.
	output, err := io.ReadAll(stdout)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("failed to read reduce output: %v", err)
		return result
	}

	if err := wait(); err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("reduce child process failed: %v", err)
		return result
	}

	// 5. Upload output to S3 as a temp object.
	bucket, prefix, err := parseS3URI(a.OutputPrefix)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("invalid output prefix: %v", err)
		return result
	}

	tempObjectKey := fmt.Sprintf("%spart-%d-%s.tmp", prefix, a.Partition, a.AttemptId)
	tempObjectURI := fmt.Sprintf("s3://%s/%s", bucket, tempObjectKey)

	_, err = w.s3Client.PutObject(ctx, bucket, tempObjectKey,
		bytes.NewReader(output), int64(len(output)),
		minio.PutObjectOptions{ContentType: "text/plain"},
	)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("failed to upload reduce output: %v", err)
		return result
	}

	result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_SUCCESS
	result.OutputObject = tempObjectURI

	slog.Info("reduce task completed",
		"job_id", a.JobId,
		"task_id", a.TaskId,
		"partition", a.Partition,
		"temp_object", tempObjectURI,
		"output_bytes", len(output),
	)

	return result
}

// executePromotion promotes a temp S3 object to its final key via CopyObject + DeleteObject.
func (w *Worker) executePromotion(ctx context.Context, a *gomrv1.PromotionAssignment) *gomrv1.TaskResult {
	result := &gomrv1.TaskResult{
		Task: &gomrv1.TaskRef{
			JobId:     a.JobId,
			TaskId:    a.TaskId,
			Phase:     gomrv1.TaskPhase_TASK_PHASE_PROMOTION,
			AttemptId: a.AttemptId,
		},
	}

	// Both TempObject and FinalObject are full S3 URIs (e.g., s3://bucket/key)
	// set by the master scheduler and reduce executor.

	tempBucket, tempKey, err := parseS3URI(a.TempObject)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("invalid temp object URI: %v", err)
		return result
	}

	_, finalKey, err := parseS3URI(a.FinalObject)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("invalid final object URI: %v", err)
		return result
	}

	slog.Info("promoting temp object to final",
		"job_id", a.JobId,
		"temp", a.TempObject,
		"final", a.FinalObject,
	)

	// CopyObject: temp → final
	_, err = w.s3Client.CopyObject(ctx,
		minio.CopyDestOptions{Bucket: tempBucket, Object: finalKey},
		minio.CopySrcOptions{Bucket: tempBucket, Object: tempKey},
	)
	if err != nil {
		result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_FAILED
		result.ErrorMessage = fmt.Sprintf("CopyObject failed: %v", err)
		return result
	}

	// DeleteObject: remove temp
	err = w.s3Client.RemoveObject(ctx, tempBucket, tempKey, minio.RemoveObjectOptions{})
	if err != nil {
		slog.Warn("failed to delete temp object (non-fatal)", "key", tempKey, "error", err)
		// Non-fatal: the final object exists. We can clean up later.
	}

	result.State = gomrv1.TaskResultState_TASK_RESULT_STATE_SUCCESS
	result.OutputObject = a.FinalObject

	slog.Info("promotion completed",
		"job_id", a.JobId,
		"final_object", a.FinalObject,
	)

	return result
}

// downloadPartition downloads a partition file from a map worker via HTTP GET
// and returns the lines as byte slices.
func downloadPartition(ctx context.Context, client *http.Client, partitionURL string) ([][]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, partitionURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, partitionURL)
	}

	var lines [][]byte
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := make([]byte, len(scanner.Bytes()))
		copy(line, scanner.Bytes())
		lines = append(lines, line)
	}
	return lines, scanner.Err()
}

// extractKey returns the key portion of a "key\tvalue" line (everything before the first tab).
func extractKey(line []byte) []byte {
	if idx := bytes.IndexByte(line, '\t'); idx >= 0 {
		return line[:idx]
	}
	return line
}

// downloadAndCompile downloads a source file from S3, runs the user's compile
// command, and returns the path to the compiled binary.
//
// The compile command supports {source} and {output} placeholders that are
// substituted with actual paths at runtime.
func (w *Worker) downloadAndCompile(ctx context.Context, sourceURI, compileCmd string) (string, error) {
	bucket, key, err := parseS3URI(sourceURI)
	if err != nil {
		return "", fmt.Errorf("invalid source URI %q: %w", sourceURI, err)
	}

	// Create work directory for this source.
	srcDir := filepath.Join(w.workDir, "sources")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create source dir: %w", err)
	}

	// Download source file.
	srcFilename := filepath.Base(key)
	srcPath := filepath.Join(srcDir, srcFilename)
	if err := w.s3Client.FGetObject(ctx, bucket, key, srcPath, minio.GetObjectOptions{}); err != nil {
		return "", fmt.Errorf("failed to download source from s3://%s/%s: %w", bucket, key, err)
	}

	// Determine output binary path.
	binDir := filepath.Join(w.workDir, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create bin dir: %w", err)
	}
	outputPath := filepath.Join(binDir, strings.TrimSuffix(srcFilename, filepath.Ext(srcFilename)))

	// Substitute placeholders in compile command.
	cmd := compileCmd
	cmd = strings.ReplaceAll(cmd, "{source}", srcPath)
	cmd = strings.ReplaceAll(cmd, "{output}", outputPath)

	slog.Info("compiling source",
		"source_uri", sourceURI,
		"compile_cmd", cmd,
	)

	// Run compile command via shell.
	compile := exec.CommandContext(ctx, "sh", "-c", cmd)
	compile.Dir = w.workDir
	if output, err := compile.CombinedOutput(); err != nil {
		return "", fmt.Errorf("compile command failed: %w\nOutput: %s", err, string(output))
	}

	// Verify binary exists and is executable.
	info, err := os.Stat(outputPath)
	if err != nil {
		return "", fmt.Errorf("compiled binary not found at %s: %w", outputPath, err)
	}
	if info.Mode()&0111 == 0 {
		// Make executable if not already.
		os.Chmod(outputPath, 0755)
	}

	return outputPath, nil
}

// runChildProcess runs a compiled binary as a child process, piping input
// to its stdin and returning a reader for its stdout.
// The caller must call the returned wait function to clean up.
func runChildProcess(ctx context.Context, binaryPath string, stdin io.Reader) (io.Reader, func() error, error) {
	cmd := exec.CommandContext(ctx, binaryPath)
	cmd.Stdin = stdin
	cmd.Stderr = os.Stderr // Forward child stderr for debugging.

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("failed to start child process: %w", err)
	}

	return stdout, cmd.Wait, nil
}

// parseS3URI parses an S3 URI (s3://bucket/key) into bucket and key components.
func parseS3URI(uri string) (bucket, key string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("invalid scheme: %s (expected s3)", u.Scheme)
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	return bucket, key, nil
}
