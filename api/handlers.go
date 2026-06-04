package api

import (
	"encoding/json"
	"log"
	"net/http"

	"go.temporal.io/sdk/client"

	wf "streak-tracker/workflow"
)

type handlers struct {
	client client.Client
}

type recordRequest struct {
	UserID      string `json:"userId"`
	Timezone    string `json:"timezone"`
	DayDuration int    `json:"dayDuration"` // demo mode: seconds per "day", 0 = real days
}

type timezoneRequest struct {
	UserID      string `json:"userId"`
	NewTimezone string `json:"newTimezone"`
}

func (h *handlers) handleRecord(w http.ResponseWriter, r *http.Request) {
	var req recordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.UserID == "" {
		http.Error(w, "userId is required", http.StatusBadRequest)
		return
	}
	if req.Timezone == "" {
		req.Timezone = "America/Chicago"
	}

	workflowID := "streak-" + req.UserID

	initialState := wf.StreakState{
		UserID:             req.UserID,
		Timezone:           req.Timezone,
		AnchorTimezone:     req.Timezone,
		DayDurationSeconds: req.DayDuration,
	}

	opts := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: wf.TaskQueue,
	}

	payload := wf.RecordActivityPayload{Timezone: req.Timezone}

	_, err := h.client.SignalWithStartWorkflow(
		r.Context(),
		workflowID,
		wf.RecordActivitySignal,
		payload,
		opts,
		wf.StreakWorkflow,
		initialState,
	)
	if err != nil {
		log.Printf("SignalWithStartWorkflow failed: %v", err)
		http.Error(w, "failed to record activity", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *handlers) handleGetStreak(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userId")
	if userID == "" {
		http.Error(w, "userId query parameter is required", http.StatusBadRequest)
		return
	}

	workflowID := "streak-" + userID

	resp, err := h.client.QueryWorkflow(r.Context(), workflowID, "", wf.GetStreakQuery)
	if err != nil {
		log.Printf("QueryWorkflow failed: %v", err)
		http.Error(w, "failed to get streak (workflow may not exist yet)", http.StatusNotFound)
		return
	}

	var state wf.StreakState
	if err := resp.Get(&state); err != nil {
		log.Printf("Failed to decode query result: %v", err)
		http.Error(w, "failed to decode streak state", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

func (h *handlers) handleChangeTimezone(w http.ResponseWriter, r *http.Request) {
	var req timezoneRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.UserID == "" || req.NewTimezone == "" {
		http.Error(w, "userId and newTimezone are required", http.StatusBadRequest)
		return
	}

	workflowID := "streak-" + req.UserID
	payload := wf.ChangeTimezonePayload{NewTimezone: req.NewTimezone}

	err := h.client.SignalWorkflow(r.Context(), workflowID, "", wf.ChangeTimezoneSignal, payload)
	if err != nil {
		log.Printf("SignalWorkflow failed: %v", err)
		http.Error(w, "failed to change timezone (record activity first to start a streak)", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
