package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	ch, err := conn.Channel()

	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.Durable,
	)

	if err != nil {
		log.Fatalf("could not game logs channel/queue: %v", err)
	}

	fmt.Printf("Queue %s declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		cmd := words[0]

		switch cmd {
		case "pause":
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})

			if err != nil {
				log.Printf("could not publish time: %s", err)
			}
			fmt.Println("Pause message sent!")
		case "resume":
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})

			if err != nil {
				log.Printf("could not publish time: %s", err)
			}
			fmt.Println("Resume message sent!")
		case "quit":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("unknow command")
		}
	}
}
