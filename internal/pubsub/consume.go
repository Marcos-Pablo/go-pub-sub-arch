package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Transient SimpleQueueType = iota
	Durable
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	dur := queueType == Durable
	autDelet := queueType == Transient
	exclusive := queueType == Transient

	queue, err := ch.QueueDeclare(queueName, dur, autDelet, exclusive, false, nil)

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)

	if err != nil {
		return err
	}

	deliveryChan, err := ch.Consume(queue.Name, "", false, false, false, false, nil)

	if err != nil {
		return err
	}

	go func() {
		defer ch.Close()
		for msg := range deliveryChan {
			var data T
			err = json.Unmarshal(msg.Body, &data)

			if err != nil {
				log.Printf("error unmarshalling mesage body: %s", err)
				continue
			}
			handler(data)
			err = msg.Ack(false)

			if err != nil {
				log.Printf("error acknowledging message: %s", err)
				continue
			}
		}
	}()
	return nil
}
