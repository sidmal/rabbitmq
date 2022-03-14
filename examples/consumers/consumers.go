package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sidmal/rabbitmq"
	"log"
)

type Message struct {
	Value string
}

func main() {
	dsn := "amqp://guest:guest@127.0.0.1:5672"
	topicName := "example"
	opts := []rabbitmq.Option{
		rabbitmq.DSN(dsn),
		rabbitmq.Topic(topicName),
	}
	broker, err := rabbitmq.NewBroker(opts...)
	if err != nil {
		log.Fatalln(err)
	}

	opts = []rabbitmq.Option{
		rabbitmq.DSN(dsn),
		rabbitmq.Topic(topicName),
		rabbitmq.Queue(&rabbitmq.QueueOptions{
			Opts: rabbitmq.DefaultQueueOpts,
			Args: amqp.Table{
				"x-dead-letter-exchange":    topicName,
				"x-message-ttl":             int32(3 * 1000),
				"x-dead-letter-routing-key": "*",
			},
		}),
		rabbitmq.Exchange(&rabbitmq.ExchangeOptions{
			Name: "example.timeout3s",
			Opts: rabbitmq.DefaultExchangeOpts,
		}),
	}
	brokerDlx, err := rabbitmq.NewBroker(opts...)
	if err != nil {
		log.Fatalln(err)
	}

	err = broker.AddConsumerHandler(&Message{}, ConsumerOne)
	if err != nil {
		log.Fatalln(err)
	}

	err = broker.AddConsumerHandler(&Message{}, ConsumerTwo)
	if err != nil {
		log.Fatalln(err)
	}

	err = broker.Publish(&Message{Value: "this message will be processed immediately"}, nil)
	if err != nil {
		log.Fatalln(err)
	}

	err = brokerDlx.Publish(&Message{Value: "this message will be processed with delay 3 second"}, nil)
	if err != nil {
		log.Fatalln(err)
	}

	_ = broker.StartConsume(nil)
}

func ConsumerOne(msg interface{}, _ amqp.Delivery) error {
	message := msg.(*Message)
	log.Println(message.Value)

	return nil
}

func ConsumerTwo(msg interface{}, _ amqp.Delivery) error {
	message := msg.(*Message)
	message.Value = "[ConsumerTwo] " + message.Value
	log.Println(message.Value)

	return nil
}
