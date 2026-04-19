package master

import (
	"fmt"
	"log"
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sophic00/gomr/internal/config"
)

func NewMaster(cfg *config.Config) (*Master, error) {
	creds := credentials.NewEnvAWS()
	if cfg.AWSAccessKeyID != "" {
		creds = credentials.NewStaticV4(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, "")
	}

	minioClient, err := minio.New(cfg.S3Endpoint, &minio.Options{
		Creds:  creds,
		Secure: false,
		Region: cfg.AWSRegion,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize S3 client: %v", err)
	}

	return &Master{
		port:     cfg.MasterPort,
		jobs:     make(map[string]*Job),
		queue:    make(chan string, 1000),
		s3Client: minioClient,
	}, nil
}

// Start initializes and runs the Gomr Master daemon on the given port.
func Start(cfg *config.Config) error {
	m, err := NewMaster(cfg)
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
