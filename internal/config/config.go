package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	MasterHTTPPort int    `mapstructure:"master_http_port"`
	MasterGRPCPort int    `mapstructure:"master_grpc_port"`
	MasterGRPCAddr string `mapstructure:"master_grpc_addr"`
	WorkerHost     string `mapstructure:"worker_host"`
	WorkerPort     int    `mapstructure:"worker_port"`

	S3Endpoint         string `mapstructure:"s3_endpoint"`
	AWSAccessKeyID     string `mapstructure:"aws_access_key_id"`
	AWSSecretAccessKey string `mapstructure:"aws_secret_access_key"`
	AWSRegion          string `mapstructure:"aws_region"`
}

func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	viper.SetDefault("master_http_port", 8080)
	viper.SetDefault("master_grpc_port", 9090)
	viper.SetDefault("master_grpc_addr", "localhost:9090")
	viper.SetDefault("worker_host", "localhost")
	viper.SetDefault("worker_port", 8081)
	viper.SetDefault("s3_endpoint", "thia:3900")

	// Read config file if present
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
