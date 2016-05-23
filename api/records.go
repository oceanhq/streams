package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"encoding/base64"

	"encoding/hex"

	"time"

	"github.com/gorilla/mux"
	"github.com/oceanhq/streams/platform"
)

var (
	RecordCollectionPostHandler = jsonResponder(recordCreate)
	RecordCollectionGetHandler  = jsonResponder(recordsIndex)
)

func recordCreate(r *http.Request) (interface{}, int) {
	// Get stream ID from path
	vars := mux.Vars(r)
	streamId := vars["stream_id"]

	// Parse the content from the request
	type requestData struct {
		Content string `json:"content"`
	}
	parsed := &requestData{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(parsed)
	if err != nil {
		return jsonError{fmt.Sprintf("JSON parse error: %s", err.Error())}, http.StatusBadRequest
	}

	content, err := base64.StdEncoding.DecodeString(parsed.Content)
	if err != nil {
		return jsonError{fmt.Sprintf("Error decoding content: %s", err.Error())}, http.StatusBadRequest
	}

	// Publish the new record to the stream
	rec, err := platformImpl.CreateRecord(streamId, content)
	if err != nil {
		code := http.StatusInternalServerError

		// In case the client supplies a non-existant stream, bad request
		if _, ok := err.(*platform.ErrStreamNotFound); ok {
			code = http.StatusBadRequest
		}

		return asJsonError(err), code
	}

	res := &recordDocument{
		RecordId:    rec.Id,
		Content:     "", // Omit content. Returning the hash is sufficient for client validation.
		ContentSha1: hex.EncodeToString(rec.ContentHash),
		Timestamp:   rec.Timestamp.Format(time.RFC3339Nano)}

	return res, http.StatusCreated
}

func recordsIndex(r *http.Request) (interface{}, int) {
	cursorId := r.Header.Get("X-Cursor-ID")

	if cursorId == "" {
		return jsonError{"X-Cursor-ID header must be specified."}, http.StatusBadRequest
	}

	// Get stream ID from path
	vars := mux.Vars(r)
	streamId := vars["stream_id"]

	recs, err := platformImpl.GetRecords(streamId, cursorId)
	if err != nil {
		code := http.StatusInternalServerError

		if _, ok := err.(*platform.ErrInvalidParam); ok {
			code = http.StatusBadRequest
		} else if _, ok := err.(*platform.ErrStreamNotFound); ok {
			code = http.StatusNotFound
		} else if _, ok := err.(*platform.ErrCursorNotFound); ok {
			code = http.StatusBadRequest
		}

		return asJsonError(err), code
	}

	res := &recordCollection{
		Records: []recordDocument{}}

	for i := 0; i < len(recs); i++ {
		rec := recs[i]

		doc := recordDocument{
			RecordId:    rec.Id,
			Content:     base64.StdEncoding.EncodeToString(rec.Content),
			ContentSha1: hex.EncodeToString(rec.ContentHash),
			Timestamp:   rec.Timestamp.Format(time.RFC3339Nano)}
		res.Records = append(res.Records, doc)
	}

	return res, http.StatusOK
}

type recordDocument struct {
	RecordId    string `json:"recordId"`
	Content     string `json:"content,omitempty"`
	ContentSha1 string `json:"contentHash"`
	Timestamp   string `json:"timestamp"`
}

type recordCollection struct {
	Records []recordDocument `json:"records"`
}
