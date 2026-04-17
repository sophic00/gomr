package worker

import (
	"fmt"
	"net/http"
)

// Start initializes and runs the Gomr Worker on the given port,
// connecting to the Master at masterAddr.
func Start(masterAddr string, port int) error {
	// TODO: Connect to Master RPC
	// TODO: Register worker ID
	// TODO: Start heartbeat goroutine
	// TODO: Start fetching and executing tasks

	addr := fmt.Sprintf(":%d", port)

	// Create a minimal HTTP server to serve intermediate files (Data Plane)
	// For now, this just serves a dummy response
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Worker HTTP Data Plane on port %d\n", port)
	})

	fmt.Printf("Worker HTTP server listening on %s (connected to master %s)\n", addr, masterAddr)

	// Start HTTP file server (blocking)
	return http.ListenAndServe(addr, nil)
}
