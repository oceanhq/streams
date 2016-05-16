package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/oceanhq/streams/platform"
	"github.com/oceanhq/streams/platform/memory"
)

func jsonResponder(f func(r *http.Request) (result interface{}, statusCode int)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		res, code := f(r)

		w.WriteHeader(code)
		w.Header().Set("Content-type", "application/json")

		output, err := json.Marshal(res)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		fmt.Fprint(w, string(output))
	}
}

func asJsonError(err error) *jsonError {
	return &jsonError{
		Error: err.Error()}
}

type jsonError struct {
	Error string `json:"error"`
}

var (
	platformImpl platform.Platform = &memory.InMemoryPlatform{}
)
