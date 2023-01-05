package main

import (
	"broker/event"
	"broker/logs"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/rpc"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RequestPayload struct {
	Action string      `json:"action"`
	Auth   AuthPayload `json:"auth,omitempty"`
	Log    LogPayload  `json:"log,omitempty"`
	Mail   MailPayload `json:"mail,omitempty"`
}

type AuthPayload struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LogPayload struct {
	Name string `json:"name"`
	Data string `json:"data"`
}

type MailPayload struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Subject string `json:"subject"`
	Message string `json:"message"`
}

func (app *Config) Broker(w http.ResponseWriter, r *http.Request) {
	payload := jsonResponse{
		Error:   false,
		Message: "Hit the broker v4",
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
		// app.logItem(w, requestPayload.Log)
		// app.logItemViaQueue(w, requestPayload.Log)
		app.logItemViaRpc(w, requestPayload.Log)
	case "mail":
		app.sendMail(w, requestPayload.Mail)
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

func (app *Config) sendMail(w http.ResponseWriter, msg MailPayload) {
	log.Printf("::sendMail - called with F:'%s' T:'%s' S:'%s' M:'%s'", msg.From, msg.To, msg.Subject, msg.Message)

	jsonData, _ := json.MarshalIndent(msg, "", "\t")

	mailServiceUrl := "http://mail-service/send"

	request, err := http.NewRequest("POST", mailServiceUrl, bytes.NewBuffer(jsonData))
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

	log.Printf("::sendMail - response from logger server, Code %d", response.StatusCode)

	if response.StatusCode != http.StatusAccepted {
		app.errorJson(w, errors.New("error calling mail service"))
		return
	}

	var payloadResponse jsonResponse
	payloadResponse.Error = false
	payloadResponse.Message = "mail sent"

	app.writeJson(w, http.StatusAccepted, payloadResponse)
}

func (app *Config) logItemViaQueue(w http.ResponseWriter, entry LogPayload) {
	log.Printf("::logItemViaQueue - called with N:'%s' D:'%s'", entry.Name, entry.Data)

	err := app.pushToQueue(entry.Name, entry.Data)
	if err != nil {
		app.errorJson(w, err)
		return
	}

	var payloadResponse jsonResponse
	payloadResponse.Error = false
	payloadResponse.Message = "logged"

	app.writeJson(w, http.StatusAccepted, payloadResponse)
}

func (app *Config) pushToQueue(name, message string) error {
	emmitter, err := event.NewEmitter(app.Rabbit)
	if err != nil {
		return err
	}

	payload := LogPayload{
		Name: name,
		Data: message,
	}

	j, _ := json.MarshalIndent(&payload, "", "\t")

	err = emmitter.Push(string(j), "log.INFO")
	if err != nil {
		return err
	}

	return nil
}

type RpcPayload struct {
	Name string
	Data string
}

func (app *Config) logItemViaRpc(w http.ResponseWriter, entry LogPayload) {
	log.Printf("::logItemViaRpc - called with N:'%s' D:'%s'", entry.Name, entry.Data)
	client, err := rpc.Dial("tcp", "logger-service:5001")
	if err != nil {
		app.errorJson(w, err)
		return
	}

	payload := RpcPayload{
		Name: entry.Name,
		Data: entry.Data,
	}

	var result string
	err = client.Call("RpcServer.LogInfo", payload, &result)
	if err != nil {
		app.errorJson(w, err)
		return
	}

	var payloadResponse jsonResponse
	payloadResponse.Error = false
	payloadResponse.Message = "logged via rpc"

	app.writeJson(w, http.StatusAccepted, payloadResponse)
}

func (app *Config) logItemViaGrpc(w http.ResponseWriter, r *http.Request) {
	log.Printf("::logItemViaGrpc")
	var requestPayload RequestPayload

	err := app.readJson(w, r, &requestPayload)
	if err != nil {
		app.errorJson(w, err)
		return
	}

	log.Printf("::logItemViaGrpc - called with N:'%s' D:'%s'", requestPayload.Log.Name, requestPayload.Log.Data)

	conn, err := grpc.Dial("logger-service:50001", grpc.WithTransportCredentials(insecure.NewCredentials().Clone()), grpc.WithBlock())
	if err != nil {
		app.errorJson(w, err)
		return
	}

	defer conn.Close()

	client := logs.NewLogServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.WriteLog(ctx, &logs.LogRequest{
		LogEntry: &logs.Log{
			Name: requestPayload.Log.Name,
			Data: requestPayload.Log.Data,
		},
	})
	if err != nil {
		app.errorJson(w, err)
		return
	}

	var payloadResponse jsonResponse
	payloadResponse.Error = false
	payloadResponse.Message = "logged via grpc!"

	app.writeJson(w, http.StatusAccepted, payloadResponse)
}
