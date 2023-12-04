package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

var db *gorm.DB

type Order struct {
	gorm.Model
	UserLineUid string `gorm:"not null"`
	Status      string `gorm:"not null"`
}

func main() {
	// Initialize the database
	initDB()

	// Initialize Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		// "bootstrap.servers": "localhost:8098",
		"bootstrap.servers": "localhost:8097,localhost:8098,localhost:8099",
		"group.id":          "message-group",
		"auto.offset.reset": "earliest",
	})
	fmt.Println("consumer==============================")
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Subscribe to the Kafka topic
	err = consumer.SubscribeTopics([]string{"kafka_topic_one"}, nil)
	if err != nil {
		log.Fatalf("Error subscribing to topic: %v", err)
	}

	// Consume messages
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Received message from topic %s: %s\n", *msg.TopicPartition.Topic, string(msg.Value))

			// Process the message
			processMessage(msg.Value)
		} else {
			fmt.Printf("Error while consuming message: %v\n", err)
		}
	}
}

func initDB() {
	var err error
	db, err = gorm.Open("mysql", "root:root@tcp(localhost)/tutorial?parseTime=true")
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}

	// Auto Migrate
	db.AutoMigrate(&Order{})
}

func processMessage(message []byte) {
	var messageData map[string]interface{}
	if err := json.Unmarshal(message, &messageData); err != nil {
		log.Printf("Error decoding JSON: %v", err)
		return
	}

	orderID, ok := messageData["orderId"].(float64)
	if !ok {
		log.Println("Error: orderId not found in the message")
		return
	}

	// Simulate a longer process with a loop
	for i := 0; i < 10; i++ {
		// Simulate some processing time
		time.Sleep(1 * time.Second)
		log.Printf("Processing... (%d/10)", i+1)
	}

	// Update the order status to "success"
	if err := db.Model(&Order{}).Where("id = ?", uint(orderID)).Update("status", "success").Error; err != nil {
		log.Printf("Error updating order status: %v", err)
		return
	}

	log.Printf("Order %d updated to success", int(orderID))
}
