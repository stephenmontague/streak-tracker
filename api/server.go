package api

import (
	"net/http"

	"go.temporal.io/sdk/client"
)

func NewServer(temporalClient client.Client) *http.ServeMux {
	h := &handlers{client: temporalClient}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/record", h.handleRecord)
	mux.HandleFunc("GET /api/streak", h.handleGetStreak)
	mux.HandleFunc("POST /api/timezone", h.handleChangeTimezone)
	mux.Handle("GET /", http.FileServer(http.Dir("static")))

	return mux
}
