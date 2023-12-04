package helper

import (
	"log"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var Producer *kafka.Producer
var Consumer *kafka.Consumer

func InitProducer(servers []string, clientID string) *kafka.Producer {
	strServer := strings.Join(servers, ",")
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strServer,
		"client.id":         clientID,
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	return p
}

func InitConsumer(servers []string, groupID string, topics ...string) *kafka.Consumer {
	strServer := strings.Join(servers, ",")
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strServer,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// Subscribe to the Kafka topics
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatalf("Error subscribing to topic: %v", err)
	}

	return c
}
