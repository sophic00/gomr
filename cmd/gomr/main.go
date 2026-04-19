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
		masterPort := masterCmd.Int("port", cfg.MasterPort, "HTTP port for the Master API and RPC")

		masterCmd.Parse(os.Args[2:])
		cfg.MasterPort = *masterPort

		log.Printf("Starting Gomr Master on port %d...", cfg.MasterPort)
		if err := master.Start(cfg); err != nil {
			log.Fatalf("Master failed: %v", err)
		}

	case "worker":
		workerCmd := flag.NewFlagSet("worker", flag.ExitOnError)
		masterAddr := workerCmd.String("master", cfg.MasterAddr, "Address of the Master node (ip:port)")
		workerPort := workerCmd.Int("port", cfg.WorkerPort, "HTTP port for the Worker to serve intermediate files")

		workerCmd.Parse(os.Args[2:])
		cfg.MasterAddr = *masterAddr
		cfg.WorkerPort = *workerPort

		log.Printf("Starting Gomr Worker on port %d, connecting to Master at %s...", cfg.WorkerPort, cfg.MasterAddr)
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
