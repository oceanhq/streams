package api

import (
	"net/http"

	"encoding/json"

	"fmt"

	"github.com/oceanhq/streams/platform"
)

var (
	StreamCollectionPostHandler = jsonResponder(streamCreate)
	StreamCollectionGetHandler  = jsonResponder(streamsIndex)
)

func streamCreate(r *http.Request) (interface{}, int) {
	// Parse the expected request body
	// Example: { "name": "tobyjsullivan/weather" }
	type requestData struct {
		Name string `json:"name"`
	}
	parsed := &requestData{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(parsed)
	if err != nil {
		return jsonError{fmt.Sprintf("JSON parse error: %s", err.Error())}, http.StatusBadRequest
	}

	// Create the actual stream on the platform
	stream, err := platformImpl.CreateStream(parsed.Name)
	if err != nil {
		code := http.StatusInternalServerError

		// platform.InvalidParamErrors imply that the client didn't provide a required value.
		// Details will be in error text.
		if _, ok := err.(platform.InvalidParamError); ok {
			code = http.StatusBadRequest
		}

		return asJsonError(err), code
	}

	res := &streamDocument{
		StreamId: stream.Id,
		Name:     stream.Name}

	// Return a success
	return res, http.StatusCreated
}

func streamsIndex(r *http.Request) (interface{}, int) {
	streams, err := platformImpl.ListStreams()
	if err != nil {
		return asJsonError(err), http.StatusInternalServerError
	}

	// Copy returned stream list into marshallable response object
	list := &streamCollection{
		Streams: make([]streamDocument, len(streams))}
	for i := 0; i < len(streams); i++ {
		stream := streams[i]

		list.Streams[i] = streamDocument{
			StreamId: stream.Id,
			Name:     stream.Name}
	}

	return list, http.StatusOK
}

type streamDocument struct {
	StreamId string `json:"streamId"`
	Name     string `json:"name"`
}

type streamCollection struct {
	Streams []streamDocument `json:"streams"`
}
