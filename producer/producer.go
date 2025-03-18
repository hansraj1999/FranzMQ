package producer

import (
	"FranzMQ/constants"
	"FranzMQ/utils"
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Config represents topic configuration
type Config struct {
	NumOfPartition int `json:"NumOfPartition"`
}

// ProduceMessage writes a message with a distributed offset mechanism
func ProduceMessage(topicName, key string, msg interface{}) (bool, NewMsgProduceResponse, error) {
	log.Println("Producing message for topic:", topicName, "Key:", key)

	if !utils.FileExists(topicName) {
		return false, NewMsgProduceResponse{}, fmt.Errorf("topic does not exist, please create the topic first")
	}

	config, err := loadConfig(topicName)
	if err != nil {
		return false, NewMsgProduceResponse{}, err
	}

	partition := utils.MurmurHashKeyToPartition(key, config.NumOfPartition)
	log.Println("Partition selected:", partition)

	offsetKey := topicName + "-" + strconv.Itoa(partition)
	offset := constants.OffsetMap.INCR(offsetKey)

	logFile, indexFile, err := openFiles(topicName, partition)
	if err != nil {
		return false, NewMsgProduceResponse{}, err
	}
	defer logFile.Close()
	defer indexFile.Close()

	// Use buffered writers
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

	timeStamp := time.Now().UnixNano()

	// Prepare log entry
	logEntry := fmt.Sprintf("%d--%d--%d--%s\n", timeStamp, partition, offset, jsonFormattedValue)

	// Fetch and update offsets in one step
	entrySize := len(logEntry)
	endOffset := constants.LogSizeMap.INCRBY(offsetKey, entrySize)
	startOffset := endOffset - entrySize

	// Write log entry
	if _, err := logWriter.WriteString(logEntry); err != nil {
		return false, NewMsgProduceResponse{}, fmt.Errorf("error writing log: %w", err)
	}

	// Prepare and write index entry
	indexEntry := fmt.Sprintf("%d--%d--%d--%d\n", timeStamp, startOffset, endOffset, offset)
	if _, err := indexWriter.WriteString(indexEntry); err != nil {
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
	basePath := fmt.Sprintf("%s%s", constants.FilesDir, topicName)
	logFilePath := fmt.Sprintf("%s/%s-%d.log", basePath, topicName, partition)
	indexFilePath := fmt.Sprintf("%s/index/%s-%d.index", basePath, topicName, partition)

	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening log file: %w", err)
	}

	indexFile, err := os.OpenFile(indexFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		_ = logFile.Close()
		return nil, nil, fmt.Errorf("error opening index file: %w", err)
	}

	return logFile, indexFile, nil
}
