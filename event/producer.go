package event

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Emmitter struct {
	connection *amqp.Connection
}

func NewEmitter(conn *amqp.Connection) (Emmitter, error) {
	emmitter := Emmitter{
		connection: conn,
	}
	
	err := emmitter.setup()
	if err != nil {
		return Emmitter{}, err
	}

	return emmitter, nil
}

func (e *Emmitter) setup() error {
	channel, err := e.connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	
	return declareExchange(channel)
}

func (e *Emmitter) push(event string, severity string) error {
	channel, err := e.connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	log.Println("Pushing to channel")

	err = channel.Publish(
		"logs_topic",
		severity,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body: []byte(event)
		},
	)
	
	if err != nil {
		return err
	}

	return nil
}