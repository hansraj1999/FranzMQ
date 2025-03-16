package main

import (
	"FranzMQ/producer"
	"FranzMQ/topic"
	"FranzMQ/utils"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // Import for side effects
	"os"
)

const dataDir = "/app/data"

type CreateTopicRequest struct {
	Name   string `json:"name"`
	Config struct {
		Compression    string `json:"compression"`
		DataType       string `json:"data_type"`
		Replicas       int    `json:"replicas"`
		NumOfPartition int    `json:"num_of_partitions"`
	} `json:"config"`
}

type ProduceMessageRequest struct {
	Topic   string      `json:"topic"`
	Key     string      `json:"key"`
	Message interface{} `json:"message"`
}

// JSON response helper
func jsonResponse(w http.ResponseWriter, statusCode int, message interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{"message": message})
}

func createTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, http.StatusBadRequest, "Invalid request payload")
		return
	}
	defer r.Body.Close()

	log.Println("Creating topic:", req.Name)

	config := topic.Config{
		Compression:        req.Config.Compression,
		DataType:           req.Config.DataType,
		Replicas:           req.Config.Replicas,
		NumOfPartition:     req.Config.NumOfPartition,
		PartitionStratergy: "HASH", // Default strategy
	}

	if success, err := topic.CreateAtTopic(req.Name, config); !success || err != nil {
		jsonResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Println("Topic created successfully:", req.Name)
	jsonResponse(w, http.StatusCreated, "Topic created successfully")
}

func produceMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ProduceMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, http.StatusBadRequest, "Invalid JSON request")
		return
	}
	defer r.Body.Close()

	log.Println("Producing message:", req)

	success, metaData, err := producer.ProduceMessage(req.Topic, req.Key,req.Message)
	if !success || err != nil {
		jsonResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Println("Message produced successfully:", metaData)
	jsonResponse(w, http.StatusOK, metaData)
}

func ensureDataDir() {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		log.Println("Data directory not found, creating...")
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			log.Fatalf("Error creating data directory: %v", err)
		}
	}
}

func main() {
	ensureDataDir()
	utils.GetEtcdClient()
	http.HandleFunc("/create-topic", createTopic)
	http.HandleFunc("/produce", produceMessage)
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	fmt.Println("🚀 FranzMQ server running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
