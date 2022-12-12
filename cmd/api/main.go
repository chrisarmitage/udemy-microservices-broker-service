package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const webPort = "80"

/**
 * The "receiver"
 */
type Config struct {
	Rabbit *amqp.Connection
}

func main() {
	// Connect to RabbitMQ
	conn, err := connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer conn.Close()
	
	app := Config{
		Rabbit: conn,
	}

	log.Printf("Starting broker service on port %s\n", webPort)

	// Define http server
	srv := &http.Server{
		Addr: fmt.Sprintf(":%s", webPort),
		Handler: app.routes(),
	}

	err = srv.ListenAndServe()
	if err != nil {
		log.Panic(err)
	}
}

func connect() (*amqp.Connection, error) {
	var counts int64
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	// Don't continue until RabbitMQ is ready
	for {
		c, err := amqp.Dial("amqp://guest:guest@rabbitmq")
		if err != nil {
			log.Println("RabbitMq not ready...")
			counts++
		} else {
			connection = c
			log.Println("Connected to RabbitMQ!")
			break // Escape the for loop
		}

		if counts > 5 {
			fmt.Println(err)
			return nil, err
		}

		// Exponential back-off
		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Printf("Backing off for %d seconds\n", int64(backOff.Seconds()))
		time.Sleep(backOff)
	}

	return connection, nil
}