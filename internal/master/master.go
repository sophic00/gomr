package master

import (
	"fmt"
	"log"
	"net/http"
	"os"

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

	mux := m.setupRoutes()

	log.Printf("Master listening on %s\n", addr)
	return http.ListenAndServe(addr, mux)
}
