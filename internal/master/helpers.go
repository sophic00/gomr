package master

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
)

// parseS3URI parses an S3 URI (e.g., s3://bucket/prefix) into bucket and prefix components.
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

// deleteS3Prefix deletes all objects under a given S3 URI prefix using batch removal.
func deleteS3Prefix(ctx context.Context, client *minio.Client, uri string) error {
	bucket, prefix, err := parseS3URI(uri)
	if err != nil {
		return err
	}

	objectsCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	// RemoveObjects accepts a channel of ObjectInfo and deletes in batches.
	removeObjCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(removeObjCh)
		for obj := range objectsCh {
			if obj.Err != nil {
				// Log but continue; errors will surface via the error channel below.
				continue
			}
			removeObjCh <- obj
		}
	}()

	for err := range client.RemoveObjects(ctx, bucket, removeObjCh, minio.RemoveObjectsOptions{}) {
		return fmt.Errorf("failed to delete %s: %v", err.ObjectName, err.Err)
	}
	return nil
}
