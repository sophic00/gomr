package config

import (
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	// Master node
	MasterHTTPPort int `mapstructure:"master_http_port"`
	MasterGRPCPort int `mapstructure:"master_grpc_port"`

	// Worker node
	MasterGRPCAddr         string        `mapstructure:"master_grpc_addr"` // used by worker to reach master
	WorkerHost             string        `mapstructure:"worker_host"`      // worker advertised address for master registration
	WorkerHTTPPort         int           `mapstructure:"worker_http_port"`
	WorkerHeartbeatInterval time.Duration `mapstructure:"worker_heartbeat_interval"`

	// Storage (S3 / MinIO / Garage)
	S3Endpoint         string `mapstructure:"s3_endpoint"`
	AWSAccessKeyID     string `mapstructure:"aws_access_key_id"`
	AWSSecretAccessKey string `mapstructure:"aws_secret_access_key"`
	AWSRegion          string `mapstructure:"aws_region"`

	// Intermediate storage
	IntermediateSpillThreshold int `mapstructure:"intermediate_spill_threshold_mb"` // MB; 0 = always in-memory
}

func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")

	// Defaults
	viper.SetDefault("master_http_port", 8080)
	viper.SetDefault("master_grpc_port", 9090)
	viper.SetDefault("master_grpc_addr", "localhost:9090")
	viper.SetDefault("worker_host", "localhost")
	viper.SetDefault("worker_http_port", 8081)
	viper.SetDefault("worker_heartbeat_interval", "5s")
	viper.SetDefault("s3_endpoint", "thia:3900")
	viper.SetDefault("intermediate_spill_threshold_mb", 256)

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
