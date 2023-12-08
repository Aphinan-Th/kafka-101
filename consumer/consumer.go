package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/jinzhu/gorm/dialects/mysql"
	db "kafka.exmaple/core"
	"kafka.exmaple/helper"
	"kafka.exmaple/models"
)

func main() {
	// Initialize the database
	db.InitDB()

	// Initialize Kafka consumer
	consumer := helper.InitConsumer([]string{"localhost:8097", "localhost:8098", "localhost:8099"}, "message-group", "kafka_topic_one")
	defer consumer.Close()

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
	if err := db.DB.Model(&models.Order{}).Where("id = ?", uint(orderID)).Update("status", "success").Error; err != nil {
		log.Printf("Error updating order status: %v", err)
		return
	}

	log.Printf("Order %d updated to success", int(orderID))
}
