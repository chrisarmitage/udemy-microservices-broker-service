package main

import (
	"net/http"
)

func (app *Config) Broker(w http.ResponseWriter, r *http.Request) {
	payload := jsonResponse{
		Error:   false,
		Message: "Hit the broker v2",
	}

	_ = app.writeJson(w, http.StatusOK, payload)
}
