// main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
	db "kafka.exmaple/core"
	"kafka.exmaple/helper"
	"kafka.exmaple/models"
)

var producer *kafka.Producer

func main() {
	// Initialize the database
	db.InitDB()

	// Initialize Kafka producer
	producer = helper.InitProducer([]string{"localhost:8097", "localhost:8098", "localhost:8099"}, "go-app")
	defer producer.Close()

	// Initialize Gorilla mux router
	router := mux.NewRouter()

	// Define routes
	router.HandleFunc("/api/create-product", createProduct).Methods("POST")
	router.HandleFunc("/api/placeorder", placeOrder).Methods("POST")

	// Start the HTTP server
	port := ":8000"
	fmt.Printf("Go app listening at http://localhost%s\n", port)
	log.Fatal(http.ListenAndServe(port, router))
}

func createProduct(w http.ResponseWriter, r *http.Request) {
	var product models.Product
	err := json.NewDecoder(r.Body).Decode(&product)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := db.DB.Create(&product).Error; err != nil {
		http.Error(w, "Failed to create product", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(product)
}

func placeOrder(w http.ResponseWriter, r *http.Request) {
	var requestData models.RequestData
	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	fmt.Printf("Received request: %+v\n", requestData)

	var product models.Product
	if err := db.DB.First(&product, requestData.ProductID).Error; err != nil {
		http.Error(w, "Product not found", http.StatusNotFound)
		return
	}

	if product.Amount <= 0 {
		http.Error(w, "Product out of stock", http.StatusBadRequest)
		return
	}

	// Reduce product amount
	product.Amount -= 1
	if err := db.DB.Save(&product).Error; err != nil {
		http.Error(w, "Failed to update product", http.StatusInternalServerError)
		return
	}

	// Create order with status pending
	order := models.Order{
		ProductID:   product.ID,
		UserLineUid: requestData.UserID,
		Status:      "pending",
	}
	if err := db.DB.Create(&order).Error; err != nil {
		http.Error(w, "Failed to create order", http.StatusInternalServerError)
		return
	}

	orderData := map[string]interface{}{
		"productName": product.Name,
		"userId":      requestData.UserID,
		"orderId":     order.ID,
	}

	message, err := json.Marshal(orderData)
	if err != nil {
		http.Error(w, "Failed to serialize order data", http.StatusInternalServerError)
		return
	}

	// Produce message to Kafka
	err = produceMessage(requestData.Topic, string(message))
	if err != nil {
		http.Error(w, "Failed to produce message to Kafka", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message": fmt.Sprintf("Buy product %s successful. Waiting message for confirmation.", product.Name),
	})
}

func produceMessage(topic, message string) error {
	deliveryChan := make(chan kafka.Event)

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, deliveryChan)

	if err != nil {
		log.Printf("Error producing message: %v", err)
		return err
	}

	// Wait for the delivery report
	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v", m.TopicPartition.Error)
		return m.TopicPartition.Error
	}

	log.Printf("Produced message to topic %s [%d] at offset %v\n",
		*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	close(deliveryChan)
	return nil
}
