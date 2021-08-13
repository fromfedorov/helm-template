package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"fmt"
	"time"
	"context"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"database/sql"

	env "github.com/Netflix/go-env"

	_ "github.com/lib/pq"

	"log"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"github.com/streadway/amqp"

	// "helm-secrets/internal/requests"
)

var (
	db    *sql.DB
	queue amqp.Queue
	ch    *amqp.Channel
)

type environment struct {
	PgsqlURI  string `env:"PGSQL_URI"`
	Listen    string `env:"LISTEN"`
	RabbitURI string `env:"RABBIT_URI"`
}

type jsonResponse struct {
	Success bool         `json:"success"`
	Message string       `json:"message"`
	Data    *interface{} `json:"data"`
}

func returnResponse(code int, msg string, data *interface{}, w http.ResponseWriter) {
	success := true
	if code >= 400 {
		success = false
	}
	respStruct := &jsonResponse{Success: success, Message: msg, Data: data}

	resp, _ := json.Marshal(respStruct)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(resp)
	w.Write([]byte("\n"))

	return
}

func main() {
	var err error

	// Getting configuration
	log.Printf("INFO: Getting environment variables\n")
	cnf := environment{}
	_, err = env.UnmarshalFromEnviron(&cnf)
	if err != nil {
		log.Fatal(err)
	}

	// Connecting to database
	log.Printf("INFO: Connecting to database")
	db, err = sql.Open("postgres", cnf.PgsqlURI)
	if err != nil {
		log.Fatalf("Can't connect to postgresql: %v", err)
	}

	// Running migrations
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Fatalf("Can't get postgres driver: %v", err)
	}
	m, err := migrate.NewWithDatabaseInstance("file:///opt/migrations", "postgres", driver)
	if err != nil {
		log.Fatalf("Can't get migration object: %v", err)
	}
	m.Up()

	// Initialising rabbit mq
	// Initing rabbitmq
	conn, err := amqp.Dial(cnf.RabbitURI)
	if err != nil {
		log.Fatalf("Can't connect to rabbitmq")
	}
	defer conn.Close()

	ch, err = conn.Channel()
	if err != nil {
		log.Fatalf("Can't open channel")
	}
	defer ch.Close()

	err = initRabbit()
	if err != nil {
		log.Fatalf("Can't create rabbitmq queues: %s\n", err)
	}

	// Setting handlers for query
	router := mux.NewRouter().StrictSlash(true)

	// PROJECTS
	router.HandleFunc("/requests", authMiddleware(getRequests)).Methods("GET")
	router.HandleFunc("/requests", authMiddleware(addRequest)).Methods("POST")
	router.HandleFunc("/requests/{name}", authMiddleware(getRequest)).Methods("GET")
	router.HandleFunc("/requests/{name}", authMiddleware(updRequest)).Methods("PUT")
	router.HandleFunc("/requests/{name}", authMiddleware(delRequest)).Methods("DELETE")

	address := fmt.Sprintf(":%s", cnf.Listen)
	log.Printf("INFO: Starting listening on %s\n", address)

	s := &http.Server{
		Addr:         address,
		Handler:      handlers.LoggingHandler(os.Stderr, router),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGTERM)

	go func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	
	select {
	case <-signals:
		log.Printf("server recieve shutdown signal...")
		// Shutdown the server when the context is canceled
		s.Shutdown(ctx)
	}
}

func notImplemented(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Not Implemented\n"))
}

func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenString := r.Header.Get("X-API-KEY")
		if tokenString != "804b95f13b714ee9912b19861faf3d25" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Missing Authorization Header\n"))
			return
		}

		next(w, r)
	})
}

func initRabbit() error {
	err := ch.ExchangeDeclare(
		"VideoParserExchange", // name
		"fanout",              // type
		true,                  // durable
		false,                 // auto delete
		false,                 // internal
		false,                 // no wait
		nil,                   // arguments
	)
	if err != nil {
		return err
	}

	err = ch.ExchangeDeclare(
		"VideoParserRetryExchange", // name
		"fanout",                   // type
		true,                       // durable
		false,                      // auto delete
		false,                      // internal
		false,                      // no wait
		nil,                        // arguments
	)
	if err != nil {
		return err
	}

	args := amqp.Table{"x-dead-letter-exchange": "VideoParserRetryExchange"}

	queue, err = ch.QueueDeclare(
		"VideoParserWorkerQueue", // name
		true,                     // durable - flush to disk
		false,                    // delete when unused
		false,                    // exclusive - only accessible by the connection that declares
		false,                    // no-wait - the queue will assume to be declared on the server
		args,                     // arguments -
	)
	if err != nil {
		return err
	}

	args = amqp.Table{"x-dead-letter-exchange": "VideoParserExchange", "x-message-ttl": 60000}
	queue, err = ch.QueueDeclare(
		"VideoParserWorkerRetryQueue", // name
		true,                          // durable - flush to disk
		false,                         // delete when unused
		false,                         // exclusive - only accessible by the connection that declares
		false,                         // no-wait - the queue will assume to be declared on the server
		args,                          // arguments -
	)
	if err != nil {
		return err
	}

	queue, err = ch.QueueDeclare(
		"VideoParserArchiveQueue", // name
		true,                      // durable - flush to disk
		false,                     // delete when unused
		false,                     // exclusive - only accessible by the connection that declares
		false,                     // no-wait - the queue will assume to be declared on the server
		nil,                       // arguments -
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind("VideoParserWorkerQueue", "*", "VideoParserExchange", false, nil)
	if err != nil {
		return err
	}

	err = ch.QueueBind("VideoParserArchiveQueue", "*", "VideoParserExchange", false, nil)
	if err != nil {
		return err
	}

	err = ch.QueueBind("VideoParserWorkerRetryQueue", "*", "VideoParserRetryExchange", false, nil)
	if err != nil {
		return err
	}

	return nil
}
