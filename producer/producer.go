package producer

import (
	"FranzMQ/constants"
	"FranzMQ/utils"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Config struct {
	NumOfPartition int `json:"NumOfPartition"`
}

type ConfigCacheEntry struct {
	config     *Config
	lastUpdate time.Time
}

// Cache for loaded configurations
var (
	configCache   sync.Map // Key: topicName, Value: ConfigCacheEntry
	cacheDuration = 10 * time.Second
)

// Ensure the queue is created before use
func getQueue(topic string, partition int) chan LogEntry {
	return logQueues[topic][partition]
}

// Produce message and push to appropriate queues
func ProduceMessage(ctx context.Context, topicName, key string, msg interface{}) (bool, NewMsgProduceResponse, error) {
	ctx, span := constants.Tracer.Start(ctx, "ProduceMessage")
	defer span.End()

	log.Println("Producing message for topic:", topicName, "Key:", key)

	exists := utils.FileExists(ctx, topicName)
	if !exists {
		return false, NewMsgProduceResponse{}, fmt.Errorf("topic does not exist, please create the topic first")
	}

	config, err := loadConfig(ctx, topicName)
	if err != nil {
		return false, NewMsgProduceResponse{}, err
	}

	partition := utils.MurmurHashKeyToPartition(ctx, key, config.NumOfPartition)
	log.Println("Partition selected:", partition)

	timeStamp := time.Now().UnixNano()

	jsonFormattedValue, err := utils.StructToJSON(ctx, msg)
	if err != nil {
		return false, NewMsgProduceResponse{}, fmt.Errorf("error converting message to JSON: %w", err)
	}

	logQueue := getQueue(topicName, partition)
	if logQueue == nil {
		return false, NewMsgProduceResponse{}, fmt.Errorf("log queue not found for topic %s and partition %d", topicName, partition)
	}

	// Send LogEntry with context
	logQueue <- LogEntry{Ctx: ctx, Entry: jsonFormattedValue}

	return true, NewMsgProduceResponse{Offset: -1, Partition: partition, TimeStamp: timeStamp}, nil
}

// Load topic configuration
func loadConfig(ctx context.Context, topicName string) (*Config, error) {
	ctx, span := constants.Tracer.Start(ctx, "loadConfig")
	defer span.End()

	now := time.Now()

	// Check if the config is cached
	if cached, ok := configCache.Load(topicName); ok {
		entry := cached.(ConfigCacheEntry)
		if now.Sub(entry.lastUpdate) < cacheDuration {
			return entry.config, nil // Return cached config
		}
	}

	// Load from disk if cache is expired or missing
	configPath := fmt.Sprintf("%s%s/%s.json", constants.FilesDir, topicName, topicName)
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("error opening config file: %w", err)
	}
	defer file.Close()

	var config Config
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return nil, fmt.Errorf("error decoding config file: %w", err)
	}

	// Store in cache
	configCache.Store(topicName, ConfigCacheEntry{config: &config, lastUpdate: now})

	return &config, nil
}
