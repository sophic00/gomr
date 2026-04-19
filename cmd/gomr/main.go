package main

import (
	"flag"
	"log"
	"os"

	"github.com/sophic00/gomr/internal/config"
	"github.com/sophic00/gomr/internal/master"
	"github.com/sophic00/gomr/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit()
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	switch os.Args[1] {
	case "master":
		masterCmd := flag.NewFlagSet("master", flag.ExitOnError)
		httpPort := masterCmd.Int("http-port", cfg.MasterHTTPPort, "HTTP port for the Master API")
		grpcPort := masterCmd.Int("grpc-port", cfg.MasterGRPCPort, "gRPC port for worker control traffic")

		masterCmd.Parse(os.Args[2:])
		cfg.MasterHTTPPort = *httpPort
		cfg.MasterGRPCPort = *grpcPort

		log.Printf("Starting Gomr Master with HTTP port %d and gRPC port %d...", cfg.MasterHTTPPort, cfg.MasterGRPCPort)
		if err := master.Start(cfg); err != nil {
			log.Fatalf("Master failed: %v", err)
		}

	case "worker":
		workerCmd := flag.NewFlagSet("worker", flag.ExitOnError)
		masterGRPCAddr := workerCmd.String("master-grpc", cfg.MasterGRPCAddr, "Address of the Master gRPC endpoint (ip:port)")
		workerHost := workerCmd.String("host", cfg.WorkerHost, "Advertised hostname or IP for the Worker's HTTP server")
		workerPort := workerCmd.Int("port", cfg.WorkerPort, "HTTP port for the Worker to serve intermediate files")

		workerCmd.Parse(os.Args[2:])
		cfg.MasterGRPCAddr = *masterGRPCAddr
		cfg.WorkerHost = *workerHost
		cfg.WorkerPort = *workerPort

		log.Printf("Starting Gomr Worker on %s:%d, connecting to Master gRPC at %s...", cfg.WorkerHost, cfg.WorkerPort, cfg.MasterGRPCAddr)
		if err := worker.Start(cfg); err != nil {
			log.Fatalf("Worker failed: %v", err)
		}

	default:
		log.Printf("Unknown command: %s", os.Args[1])
		printUsageAndExit()
	}
}

func printUsageAndExit() {
	log.Print("Usage: gomr <command> [arguments]\n\n")
	log.Print("The commands are:\n")
	log.Print("  master    Start the Gomr Master node\n")
	log.Print("  worker    Start a Gomr Worker node\n\n")
	log.Fatal("Use 'gomr <command> -h' for more information about a command.")
}
