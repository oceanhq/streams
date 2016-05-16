package main

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/oceanhq/streams/api"
)

func buildRoutes() http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/streams", api.StreamCollectionPostHandler).
		Methods("POST")
	r.HandleFunc("/streams", api.StreamCollectionGetHandler).
		Methods("GET")
	r.HandleFunc("/streams/{stream_id}/records", api.RecordCollectionGetHandler).
		Methods("GET")
	r.HandleFunc("/streams/{stream_id}/records", api.RecordCollectionPostHandler).
		Methods("POST")
	r.HandleFunc("/streams/{stream_id}/cursors", api.CursorCollectionPostHandler).
		Methods("POST")

	return r
}
