package main

import (
	"FranzMQ/constants"
	"FranzMQ/producer"
	"FranzMQ/topic"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // Import for side effects
	"os"
	"time"

	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.22.0"
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
	ctx, span := constants.Tracer.Start(context.Background(), "produceMessage POST")
	defer span.End()
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

	success, metaData, err := producer.ProduceMessage(ctx, req.Topic, req.Key, req.Message)
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

func startTracing() (*trace.TracerProvider, error) {
	headers := map[string]string{
		"content-type": "application/json",
	}

	exporter, err := otlptrace.New(
		context.Background(),
		otlptracehttp.NewClient(
			otlptracehttp.WithEndpoint("jaeger:4318"),
			otlptracehttp.WithHeaders(headers),
			otlptracehttp.WithInsecure(),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating new exporter: %w", err)
	}

	tracerprovider := trace.NewTracerProvider(
		trace.WithBatcher(
			exporter,
			trace.WithMaxExportBatchSize(trace.DefaultMaxExportBatchSize),
			trace.WithBatchTimeout(trace.DefaultScheduleDelay*time.Millisecond),
			trace.WithMaxExportBatchSize(trace.DefaultMaxExportBatchSize),
		),
		trace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("FranzMQ"),
			),
		),
	)

	otel.SetTracerProvider(tracerprovider)
	return tracerprovider, nil
}

func main() {

	tp, err := startTracing()
	if err != nil {
		log.Fatalf("failed to initialize tracer: %v", err)
	}
	defer func() { _ = tp.Shutdown(context.Background()) }()

	constants.Tracer = otel.Tracer("franzmq")

	ensureDataDir()
	http.HandleFunc("/create-topic", createTopic)
	http.HandleFunc("/produce", produceMessage)
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	fmt.Println("🚀 FranzMQ server running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
