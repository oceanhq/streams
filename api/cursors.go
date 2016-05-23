package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/oceanhq/streams/platform"
)

var CursorCollectionPostHandler = jsonResponder(cursorCreate)

func cursorCreate(r *http.Request) (interface{}, int) {
	// Get stream ID from path
	vars := mux.Vars(r)
	streamId := vars["stream_id"]

	// Create the new cursor
	cursor, err := platformImpl.CreateCursor(streamId)
	if err != nil {
		code := http.StatusInternalServerError

		// In case the client supplies a non-existant stream, bad request
		if _, ok := err.(*platform.ErrStreamNotFound); ok {
			code = http.StatusBadRequest
		}

		return asJsonError(err), code
	}

	resp := &cursorDocument{
		CursorId: cursor.Id,
		StreamId: cursor.StreamId,
		Position: cursor.Position}

	return resp, http.StatusCreated
}

type cursorDocument struct {
	CursorId string `json:"cursorId"`
	StreamId string `json:"streamId"`
	Position string `json:"position"`
}
