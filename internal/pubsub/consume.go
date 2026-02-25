package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Transient SimpleQueueType = iota
	Durable
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
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

	queue, err := ch.QueueDeclare(queueName, dur, autDelet, exclusive, false, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
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
			data, err := unmarshaller(msg.Body)

			if err != nil {
				log.Printf("error unmarshalling mesage body: %s", err)
				continue
			}
			ackType := handler(data)
			switch ackType {
			case Ack:
				err = msg.Ack(false)
				if err != nil {
					log.Printf("unable to ack the message: %s", err)
					continue
				}
				fmt.Println("Successfully acked message")
			case NackRequeue:
				err = msg.Nack(false, true)
				if err != nil {
					log.Printf("unable to nack requeue the message: %s", err)
					continue
				}
				fmt.Println("Successfully nack-requeued message")
			case NackDiscard:
				err = msg.Nack(false, false)
				if err != nil {
					log.Printf("unable to nack discard the message: %s", err)
					continue
				}
				fmt.Println("Successfully nack-discarded message")
			}

		}
	}()
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaler := func(data []byte) (T, error) {
		var body T
		err := json.Unmarshal(data, &body)

		if err != nil {
			return body, err
		}

		return body, nil
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaler)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaler := func(data []byte) (T, error) {
		var msg T
		buff := bytes.NewBuffer(data)
		err := gob.NewDecoder(buff).Decode(&msg)

		if err != nil {
			return msg, err
		}

		return msg, nil
	}
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaler)
}
