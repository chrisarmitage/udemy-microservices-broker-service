package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

type RequestPayload struct {
	Action string      `json:"action"`
	Auth   AuthPayload `json:"auth,omitempty"`
	Log    LogPayload  `json:"log,omitempty"`
}

type AuthPayload struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LogPayload struct {
	Name string `json:"name"`
	Data string `json:"data"`
}

func (app *Config) Broker(w http.ResponseWriter, r *http.Request) {
	payload := jsonResponse{
		Error:   false,
		Message: "Hit the broker v2",
	}

	_ = app.writeJson(w, http.StatusOK, payload)
}

func (app *Config) HandleSubmission(w http.ResponseWriter, r *http.Request) {
	var requestPayload RequestPayload

	err := app.readJson(w, r, &requestPayload)
	if err != nil {
		app.errorJson(w, err, http.StatusBadRequest)
		return
	}

	switch requestPayload.Action {
	case "auth":
		app.authenticate(w, requestPayload.Auth)
	case "log":
		app.logItem(w, requestPayload.Log)
	default:
		app.errorJson(w, errors.New("unknown action"), http.StatusBadRequest)
	}
}

func (app *Config) authenticate(w http.ResponseWriter, a AuthPayload) {
	log.Printf("::authenticate - called with E:'%s' P:'%s'", a.Email, a.Password)

	jsonData, _ := json.MarshalIndent(a, "", "\t")

	request, err := http.NewRequest("POST", "http://authentication-service/authenticate", bytes.NewBuffer(jsonData))
	if err != nil {
		app.errorJson(w, err)
		return
	}

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		app.errorJson(w, err)
		return
	}

	defer response.Body.Close()

	log.Printf("::authenticate - response from auth server, Code %d", response.StatusCode)

	if response.StatusCode == http.StatusUnauthorized {
		app.errorJson(w, errors.New("invalid credentials"))
		return
	} else if response.StatusCode != http.StatusOK {
		app.errorJson(w, errors.New("error calling auth service"))
		return
	}

	var jsonFromService jsonResponse
	err = json.NewDecoder(response.Body).Decode(&jsonFromService)
	if err != nil {
		app.errorJson(w, err)
		return
	}

	if jsonFromService.Error == true {
		app.errorJson(w, err, http.StatusUnauthorized)
		return
	}

	var payloadResponse jsonResponse
	payloadResponse.Error = false
	payloadResponse.Message = "Authenticated!"
	payloadResponse.Data = jsonFromService.Data

	app.writeJson(w, http.StatusOK, payloadResponse)
}

func (app *Config) logItem(w http.ResponseWriter, entry LogPayload) {
	log.Printf("::logItem - called with N:'%s' D:'%s'", entry.Name, entry.Data)

	jsonData, _ := json.MarshalIndent(entry, "", "\t")

	logServiceUrl := "http://logger-service/log"

	request, err := http.NewRequest("POST", logServiceUrl, bytes.NewBuffer(jsonData))
	if err != nil {
		app.errorJson(w, err)
		return
	}

	request.Header.Set("Content-Type", "application.json")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		app.errorJson(w, err)
		return
	}

	defer response.Body.Close()

	log.Printf("::logItem - response from logger server, Code %d", response.StatusCode)

	if response.StatusCode != http.StatusAccepted {
		app.errorJson(w, errors.New("error calling logger service"))
		return
	}

	var payloadResponse jsonResponse
	payloadResponse.Error = false
	payloadResponse.Message = "logged"

	app.writeJson(w, http.StatusAccepted, payloadResponse)
}
