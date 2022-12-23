package loghttp

import (
	"net/http"

	"github.com/gorilla/mux"
)

func NewServer(addr string) *http.Server {

	handler := NewHttpHandler()
	r := mux.NewRouter()
	r.HandleFunc("/", handler.handleProduce).Methods("POST")
	r.HandleFunc("/", handler.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}
