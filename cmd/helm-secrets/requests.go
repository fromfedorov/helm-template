package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/streadway/amqp"
)

type Request struct {
	ID int `json:"-"`

	Name        string `json:"name"`
	Description string `json:"description"`

	VideoURL string `json:"video_url"`
	TextURL  string `json:"text_url"`

	Archived  bool `json:"archived"`
	Processed bool `json:"processed"`

	CreatedAt *time.Time `json:"created_at"`
	UpdatedAt *time.Time `json:"updated_at"`
}

type Requests []Request

func (r *Request) isExist() (bool, error) {
	var requestsCount int

	stmt := `SELECT count(id) FROM requests WHERE name = $1 AND NOT archived`
	err := db.QueryRow(stmt, r.Name).Scan(&requestsCount)
	if err != nil {
		return false, err
	}

	if requestsCount == 0 {
		return false, nil
	}

	return true, nil
}

func (r *Request) load() error {
	stmt := `SELECT id, name, description, processed, video_url, text_url, created_at, updated_at FROM requests WHERE name = $1 AND NOT archived`
	err := db.QueryRow(stmt, r.Name).Scan(&r.ID, &r.Name, &r.Description, &r.Processed, &r.VideoURL, &r.TextURL, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return err
	}

	return nil
}

// Add new device or recreate the old one
type postRequestRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	Processed   *bool   `json:"processed"`
	VideoURL    *string `json:"video_url"`
	TextURL     *string `json:"text_url"`
}

func addRequest(w http.ResponseWriter, r *http.Request) {
	// Parsing event
	req := postRequestRequest{}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Printf("WARNING: Can't parse incoming request: %s\n", err)
		returnResponse(400, "Can't parse json", nil, w)
		return
	}

	request := Request{}

	if req.Name == nil {
		returnResponse(400, "name can't be null", nil, w)
		return
	}
	request.Name = *req.Name

	if req.Description != nil {
		request.Description = *req.Description
	}

	if req.Processed != nil {
		request.Processed = *req.Processed
	}

	if req.VideoURL != nil {
		request.VideoURL = *req.VideoURL
	}

	if req.TextURL != nil {
		request.TextURL = *req.TextURL
	}

	// Publishing data to rabbitmq
	msg, err := json.Marshal(request)
	if err != nil {
		log.Printf("ERROR: Marshaling request: %s\n", err)
		returnResponse(500, "Can't marshal request ", nil, w)
		return
	}

	err = ch.Publish(
		"VideoParserExchange", // exchange
		"",                    // routing key
		false,                 // mandatory - could return an error if there are no consumers or queue
		false,                 // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         msg,
		})

	if err != nil {
		log.Printf("ERROR: Publishing to rabbit: %s\n", err)
		returnResponse(500, "Can't publish to rabbit ", nil, w)
		return
	}

	stmt := `INSERT INTO requests (name, description, processed, video_url, text_url) VALUES ($1, $2, $3, $4, $5) RETURNING id`
	err = db.QueryRow(stmt, &request.Name, &request.Description, &request.Processed, &request.VideoURL, &request.TextURL).Scan(&request.ID)
	if err != nil {
		log.Printf("ERROR: Adding new request to database: %s\n", err)
		returnResponse(500, "Can't add new request ", nil, w)
		return
	}

	returnResponse(200, "Successfully added new request", nil, w)
}

func getRequests(w http.ResponseWriter, r *http.Request) {
	stmt := `SELECT id, name, description, created_at, updated_at FROM requests WHERE not archived`
	rows, err := db.Query(stmt)
	switch {
	case err == sql.ErrNoRows:
		returnResponse(200, "No requests found", nil, w)
		return
	case err != nil:
		log.Printf("ERROR: Can't get requests: %s\n", err)
		returnResponse(500, "Can't get requests", nil, w)
		return
	}
	defer rows.Close()

	var requests Requests
	for rows.Next() {
		request := Request{}
		err = rows.Scan(&request.ID, &request.Name, &request.Description, &request.CreatedAt, &request.UpdatedAt)
		if err != nil {
			log.Printf("ERROR: Can't scan request: %s\n", err)
			returnResponse(500, "Can't scan request", nil, w)
			return
		}
		requests = append(requests, request)
	}

	err = rows.Err()
	if err != nil {
		log.Printf("ERROR: Can't scan request: %s\n", err)
		returnResponse(500, "Can't scan request", nil, w)
		return
	}

	var data interface{}
	data = requests
	returnResponse(200, "Successfully got all the requests", &data, w)
}

func getRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	request := Request{}
	request.Name = vars["name"]

	ok, err := request.isExist()
	if err != nil {
		log.Printf("ERROR: Can't check if request exists: %s\n", err)
		returnResponse(500, "Can't check request", nil, w)
		return
	}
	if !ok {
		returnResponse(400, "Request doesn't exist", nil, w)
		return
	}

	err = request.load()
	if err != nil {
		log.Printf("ERROR: Can't load request: %s\n", err)
		returnResponse(500, "Can't load request", nil, w)
		return
	}

	var data interface{}
	data = request
	returnResponse(200, "Successfully got the request info", &data, w)
}

func updRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	request := Request{}
	request.Name = vars["name"]

	ok, err := request.isExist()
	if err != nil {
		log.Printf("ERROR: Can't check if request exists: %s\n", err)
		returnResponse(500, "Can't check request", nil, w)
		return
	}
	if !ok {
		returnResponse(400, "Request doesn't exist", nil, w)
		return
	}

	err = request.load()
	if err != nil {
		returnResponse(500, "Can't load request info", nil, w)
		return
	}

	// Parsing event
	req := postRequestRequest{}
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Printf("WARNING: Can't parse incoming request: %s\n", err)
		returnResponse(400, "Can't parse json", nil, w)
		return
	}

	if req.Description != nil {
		stmt := `UPDATE requests SET description = $1, updated_at = now() WHERE id = $2`
		_, err = db.Exec(stmt, req.Description, request.ID)
		if err != nil {
			returnResponse(500, "Error updating request", nil, w)
			return
		}
	}

	if req.Processed != nil {
		stmt := `UPDATE requests SET processed = $1, updated_at = now() WHERE id = $2`
		_, err = db.Exec(stmt, req.Processed, request.ID)
		if err != nil {
			returnResponse(500, "Error updating request", nil, w)
			return
		}
	}

	if req.TextURL != nil {
		stmt := `UPDATE requests SET text_url = $1, updated_at = now() WHERE id = $2`
		_, err = db.Exec(stmt, req.TextURL, request.ID)
		if err != nil {
			returnResponse(500, "Error updating request", nil, w)
			return
		}
	}

	returnResponse(200, "Successfully updated request", nil, w)
}

func delRequest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	request := Request{}
	request.Name = vars["name"]

	// Checking if device exists
	ok, err := request.isExist()
	if err != nil {
		log.Printf("ERROR: Can't check if request exists: %s\n", err)
		returnResponse(500, "Can't check request", nil, w)
		return
	}
	if !ok {
		returnResponse(400, "Request doesn't exist", nil, w)
		return
	}

	stmt := `UPDATE requests SET archived = true, updated_at = now() WHERE name = $1`
	_, err = db.Exec(stmt, request.Name)
	if err != nil {
		returnResponse(500, "Error deleting request", nil, w)
		return
	}

	returnResponse(200, "Successfully deleted request", nil, w)
}
