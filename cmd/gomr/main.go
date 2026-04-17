package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/sophic00/gomr/internal/master"
	"github.com/sophic00/gomr/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit()
	}

	switch os.Args[1] {
	case "master":
		masterCmd := flag.NewFlagSet("master", flag.ExitOnError)
		masterPort := masterCmd.Int("port", 8080, "HTTP port for the Master API and RPC")

		masterCmd.Parse(os.Args[2:])

		fmt.Printf("Starting Gomr Master on port %d...\n", *masterPort)
		if err := master.Start(*masterPort); err != nil {
			fmt.Fprintf(os.Stderr, "Master failed: %v\n", err)
			os.Exit(1)
		}

	case "worker":
		workerCmd := flag.NewFlagSet("worker", flag.ExitOnError)
		masterAddr := workerCmd.String("master", "localhost:8080", "Address of the Master node (ip:port)")
		workerPort := workerCmd.Int("port", 8081, "HTTP port for the Worker to serve intermediate files")

		workerCmd.Parse(os.Args[2:])

		fmt.Printf("Starting Gomr Worker on port %d, connecting to Master at %s...\n", *workerPort, *masterAddr)
		if err := worker.Start(*masterAddr, *workerPort); err != nil {
			fmt.Fprintf(os.Stderr, "Worker failed: %v\n", err)
			os.Exit(1)
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		printUsageAndExit()
	}
}

func printUsageAndExit() {
	fmt.Fprintf(os.Stderr, "Usage: gomr <command> [arguments]\n\n")
	fmt.Fprintf(os.Stderr, "The commands are:\n")
	fmt.Fprintf(os.Stderr, "  master    Start the Gomr Master node\n")
	fmt.Fprintf(os.Stderr, "  worker    Start a Gomr Worker node\n\n")
	fmt.Fprintf(os.Stderr, "Use 'gomr <command> -h' for more information about a command.\n")
	os.Exit(1)
}
