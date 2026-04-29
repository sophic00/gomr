package worker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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
			JobId:  a.JobId,
			TaskId: a.TaskId,
			Phase:  gomrv1.TaskPhase_TASK_PHASE_MAP,
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

	// 3. Run child process: stdin=S3 stream, capture stdout.
	stdout, wait, err := runChildProcess(ctx, binaryPath, inputObj)
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

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if err := ps.Write(scanner.Bytes()); err != nil {
			slog.Warn("partition write error", "error", err)
		}
	}
	if err := scanner.Err(); err != nil {
		slog.Warn("scanner error reading child stdout", "error", err)
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
