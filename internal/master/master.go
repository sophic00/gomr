package master

import (
	"fmt"
	"net/http"
)

// Start initializes and runs the Gomr Master daemon on the given port.
func Start(port int) error {
	// TODO: Setup RPC server for workers to register and receive tasks
	// TODO: Setup HTTP handlers for job submission API

	addr := fmt.Sprintf(":%d", port)

	// Example placeholder for the HTTP API
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "Job submission received\n")
	})

	fmt.Printf("Master listening on %s\n", addr)
	// For now, this will block forever and serve HTTP requests
	return http.ListenAndServe(addr, nil)
}
