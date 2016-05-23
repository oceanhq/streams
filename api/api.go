package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func jsonResponder(f func(r *http.Request) (result interface{}, statusCode int)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		res, code := f(r)

		// Headers MUST be set before WriteHeader or Write is called.
		w.Header().Set("Content-type", "application/json")

		// WriteHeader MUST be set before any calls to Write.
		w.WriteHeader(code)

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
