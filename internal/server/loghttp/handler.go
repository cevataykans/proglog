package loghttp

import (
	"encoding/json"
	"io"
	"net/http"
)

type httpHandler struct {
	logger *Log
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func NewHttpHandler() *httpHandler {
	return &httpHandler{
		logger: NewLogger(),
	}
}

func drainAndClose(r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	_ = r.Body.Close()
}

func (h *httpHandler) handleProduce(w http.ResponseWriter, r *http.Request) {
	defer drainAndClose(r)

	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	off, err := h.logger.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{
		Offset: off,
	}
	err = json.NewEncoder(w).Encode(&res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *httpHandler) handleConsume(w http.ResponseWriter, r *http.Request) {
	defer drainAndClose(r)

	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Offset >= uint64(len(h.logger.records)) {
		http.Error(w, "index out of range", http.StatusBadRequest)
		return
	}

	res := ConsumeResponse{
		Record: h.logger.records[req.Offset],
	}

	err = json.NewEncoder(w).Encode(&res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
