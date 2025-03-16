package producer

import (
	"FranzMQ/constants"
	"FranzMQ/utils"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Config represents topic configuration
type Config struct {
	NumOfPartition int `json:"NumOfPartition"`
}

// ProduceMessage writes a message with a distributed offset mechanism
func ProduceMessage(topicName string, key string, msg interface{}) (bool, NewMsgProduceResponse, error) {
	log.Println("Starting message production for topic:", topicName, "Key:", key)

	if !utils.FileExists(topicName) {
		return false, NewMsgProduceResponse{}, fmt.Errorf("topic does not exist, please create the topic first")
	}

	config, err := loadConfig(topicName)
	if err != nil {
		return false, NewMsgProduceResponse{}, err
	}

	partition := utils.MurmurHashKeyToPartition(key, config.NumOfPartition)
	log.Println("Selected Partition:", partition)

	// Fetch unique offset from etcd
	offset, err := GetNextOffset(topicName, partition)
	if err != nil {
		return false, NewMsgProduceResponse{}, err
	}

	logFile, indexFile, err := openFiles(topicName, partition)
	if err != nil {
		return false, NewMsgProduceResponse{}, err
	}
	defer logFile.Close()
	defer indexFile.Close()

	// Use buffered writers for efficient writes
	logWriter := bufio.NewWriter(logFile)
	indexWriter := bufio.NewWriter(indexFile)
	defer func() {
		_ = logWriter.Flush()
		_ = indexWriter.Flush()
	}()

	// Convert message to JSON
	jsonFormattedValue, err := utils.StructToJSON(msg)
	if err != nil {
		return false, NewMsgProduceResponse{}, fmt.Errorf("error converting message to JSON: %w", err)
	}

	// Capture timestamp once
	timeStamp := time.Now().UnixNano()

	// Prepare log entry efficiently
	var logEntry strings.Builder
	logEntry.Grow(64 + len(jsonFormattedValue)) // Pre-allocate buffer
	logEntry.WriteString(fmt.Sprintf("%d--%d--%d--%s\n", timeStamp, partition, offset, jsonFormattedValue))

	// Fetch and update offsets
	startOffset, endOffset, err := GetAndIncrementLogMessageOffset(topicName, partition, logEntry.Len())
	if err != nil {
		return false, NewMsgProduceResponse{}, fmt.Errorf("error updating offset: %w", err)
	}

	// Write log entry to buffer
	if _, err := logWriter.WriteString(logEntry.String()); err != nil {
		return false, NewMsgProduceResponse{}, fmt.Errorf("error writing log: %w", err)
	}

	// Prepare index entry efficiently
	var indexEntry strings.Builder
	indexEntry.Grow(64)
	indexEntry.WriteString(fmt.Sprintf("%d--%d--%d--%d\n", timeStamp, startOffset, endOffset, offset))

	// Write index entry to buffer
	if _, err := indexWriter.WriteString(indexEntry.String()); err != nil {
		return false, NewMsgProduceResponse{}, fmt.Errorf("error writing index: %w", err)
	}

	return true, NewMsgProduceResponse{Offset: offset, Partition: partition, TimeStamp: timeStamp}, nil
}

// loadConfig loads the partition config for the topic
func loadConfig(topicName string) (*Config, error) {
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
	return &config, nil
}

// openFiles opens log and index files
func openFiles(topicName string, partition int) (*os.File, *os.File, error) {
	logFilePath := fmt.Sprintf("%s%s/%s-%d.log", constants.FilesDir, topicName, topicName, partition)
	indexFilePath := fmt.Sprintf("%s%s/index/%s-%d.index", constants.FilesDir, topicName, topicName, partition)

	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening log file: %w", err)
	}

	indexFile, err := os.OpenFile(indexFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		logFile.Close()
		return nil, nil, fmt.Errorf("error opening index file: %w", err)
	}

	return logFile, indexFile, nil
}

// GetNextOffset fetches and increments the offset atomically using etcd
func GetNextOffset(topicName string, partition int) (int, error) {
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		return -1, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	key := fmt.Sprintf("/offsets/%s/%d", topicName, partition)

	// Atomic transaction to fetch and increment offset
	resp, err := etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), ">", 0)).
		Then(
			clientv3.OpGet(key),
		).
		Else(clientv3.OpPut(key, "1")).Commit()

	if err != nil {
		return -1, fmt.Errorf("etcd transaction error: %w", err)
	}

	if !resp.Succeeded {
		return 1, nil // First offset
	}

	currentOffset, err := strconv.Atoi(string(resp.Responses[0].GetResponseRange().Kvs[0].Value))
	if err != nil {
		return -1, fmt.Errorf("error parsing offset: %w", err)
	}

	// Increment and store offset atomically
	nextOffset := currentOffset + 1
	_, err = etcdClient.Put(ctx, key, fmt.Sprint(nextOffset))
	if err != nil {
		return -1, fmt.Errorf("failed to update offset: %w", err)
	}

	log.Println("New offset assigned:", nextOffset)
	return nextOffset, nil
}

func GetAndIncrementLogMessageOffset(topicName string, partition, size int) (int, int, error) {
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		return -1, -1, err
	}

	key := fmt.Sprintf("/offsets/%s/%d/start/offset", topicName, partition)
	maxRetries := 5 // Number of retries before giving up

	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// Get the current offset
		resp, err := etcdClient.Get(ctx, key)
		if err != nil {
			return -1, -1, fmt.Errorf("etcd get error: %w", err)
		}

		var currentOffset int
		if len(resp.Kvs) == 0 {
			// Initialize if the key doesn't exist
			currentOffset = 0
		} else {
			currentOffset, err = strconv.Atoi(string(resp.Kvs[0].Value))
			if err != nil {
				return -1, -1, fmt.Errorf("error parsing offset: %w", err)
			}
		}

		nextOffset := currentOffset + size // Compute new offset

		// Attempt atomic update
		updateResp, err := etcdClient.Txn(ctx).
			If(clientv3.Compare(clientv3.Value(key), "=", fmt.Sprint(currentOffset))). // Ensure no concurrent writes
			Then(clientv3.OpPut(key, fmt.Sprint(nextOffset))).                         // Update offset
			Commit()

		if err != nil {
			return -1, -1, fmt.Errorf("etcd transaction error: %w", err)
		}

		if updateResp.Succeeded {
			log.Println("Assigned new offset range:", currentOffset, "to", nextOffset)
			return currentOffset, nextOffset, nil
		}

		// Log and retry if transaction failed due to concurrent modification
		log.Printf("Offset conflict detected, retrying... attempt %d/%d", i+1, maxRetries)
		time.Sleep(10 * time.Millisecond) // Small delay before retrying
	}

	return -1, -1, fmt.Errorf("failed to update offset after multiple retries due to concurrent modification")
}
