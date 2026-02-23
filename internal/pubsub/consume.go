package pubsub

import (
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
