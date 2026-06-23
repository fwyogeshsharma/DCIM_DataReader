// Package rest holds the DCS admin REST handlers. commands.go exposes the
// downstream control-plane queue (device_commands) to EDR: EDR pulls pending
// field writes and acks the outcome. These endpoints are registered on the
// existing admin mux in main.go and guarded by a shared X-Command-Key.
package rest

import (
	"encoding/json"
	"net/http"
	"strconv"

	"go.uber.org/zap"

	"github.com/faberwork/fwdcs/internal/store"
)

// CommandHandlers builds the pull + ack HTTP handlers over the command store.
type CommandHandlers struct {
	db  *store.DB
	key string
	log *zap.Logger
}

// NewCommandHandlers constructs the handler set. key guards every request via
// the X-Command-Key header; an empty key disables the check (dev only).
func NewCommandHandlers(db *store.DB, key string, log *zap.Logger) *CommandHandlers {
	return &CommandHandlers{db: db, key: key, log: log}
}

// Register attaches the handlers to mux under /admin/commands.
func (h *CommandHandlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("/admin/commands", h.handlePull)
	mux.HandleFunc("/admin/commands/ack", h.handleAck)
}

func (h *CommandHandlers) authorized(r *http.Request) bool {
	if h.key == "" {
		return true
	}
	return r.Header.Get("X-Command-Key") == h.key
}

// GET /admin/commands?limit=N → { "commands": [...] }
func (h *CommandHandlers) handlePull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !h.authorized(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	cmds, err := h.db.PendingCommands(r.Context(), limit)
	if err != nil {
		h.log.Warn("commands pull: query failed", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if cmds == nil {
		cmds = []store.DeviceCommand{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"commands": cmds})
}

type ackRequest struct {
	ID      int64  `json:"id"`
	Applied bool   `json:"applied"`
	Error   string `json:"error"`
}

// POST /admin/commands/ack  body: {id, applied, error}
func (h *CommandHandlers) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !h.authorized(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	var req ackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := h.db.MarkCommand(r.Context(), req.ID, req.Applied, req.Error); err != nil {
		h.log.Warn("commands ack: update failed", zap.Int64("id", req.ID), zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
