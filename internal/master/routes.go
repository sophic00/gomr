package master

import "net/http"

// setupRoutes registers all the HTTP endpoints for the Master node.
func (m *Master) setupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Go 1.22+ standard HTTP routing
	mux.HandleFunc("POST /submit", m.handleSubmit)
	mux.HandleFunc("GET /status", m.handleStatus)
	mux.HandleFunc("DELETE /jobs/{id}", m.handleDelete)

	return mux
}
