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

// deleteS3Prefix deletes all objects under a given S3 URI prefix.
func deleteS3Prefix(ctx context.Context, client *minio.Client, uri string) error {
	bucket, prefix, err := parseS3URI(uri)
	if err != nil {
		return err
	}

	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	// List objects and delete them one by one
	objectsCh := client.ListObjects(ctx, bucket, opts)
	for object := range objectsCh {
		if object.Err != nil {
			return object.Err
		}
		err = client.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete %s: %v", object.Key, err)
		}
	}
	return nil
}
