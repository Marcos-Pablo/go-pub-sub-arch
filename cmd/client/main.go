package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(rabbitConnString)

	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	publishCh, err := conn.Channel()

	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	fmt.Println("Peril client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()

	if err != nil {
		log.Fatalf("Error prompting username: %s", err)
	}

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilDirect,
		"pause."+username,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs),
	)

	if err != nil {
		log.Fatalf("Error subscribing to pause topic: %s", err)
	}

	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilTopic,
		"army_moves."+username,
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handlerMove(gs, publishCh),
	)

	if err != nil {
		log.Fatalf("Error subscribing to army moves topic: %s", err)
	}

	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gs, publishCh),
	)

	if err != nil {
		log.Fatalf("Error subscribing to war topic: %s", err)
	}

	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err := gs.CommandSpawn(words)
			if err != nil {
				log.Print(err)
				continue
			}
		case "move":
			armyMv, err := gs.CommandMove(words)

			if err != nil {
				log.Print(err)
				continue
			}

			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, "army_moves."+username, armyMv)

			if err != nil {
				log.Print(err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(armyMv.Units), armyMv.ToLocation)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) != 2 {
				fmt.Println("usage spam <number>")
				continue
			}

			n, err := strconv.Atoi(words[1])

			if err != nil {
				fmt.Println("usage spam <number>")
				continue
			}

			for range n {
				msg := gamelogic.GetMaliciousLog()
				err = pubsub.PublishGob(publishCh, routing.ExchangePerilTopic, "game_logs."+username, routing.GameLog{
					CurrentTime: time.Now(),
					Message:     msg,
					Username:    username,
				})

				if err != nil {
					fmt.Printf("error spamming game log: %s\n", err)
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}

func PublishGameLog(ch *amqp.Channel, username, msg string) error {
	return pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    username,
	})
}
