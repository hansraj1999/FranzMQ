package producer

import (
	"FranzMQ/constants"
	"FranzMQ/utils"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/context"
)

type Config struct {
	NumOfPartition int `json:"NumOfPartition"`
}

// ProduceMessage handles message production with distributed offset management
func ProduceMessage(topicName, key, msg string) (bool, NewMsgProduceResponse, error) {
	var response NewMsgProduceResponse
	log.Println("Starting message production")
	log.Println("Topic Name:", topicName, "Key:", key)
	if !utils.FileExists(topicName) {
		return false, response, fmt.Errorf("topic does not exist, please create the topic first")
	}

	config, err := loadConfig(topicName)
	if err != nil {
		return false, response, err
	}

	partition := utils.MurmurHashKeyToPartition(key, config.NumOfPartition)
	log.Println("Partition selected:", partition)

	logFile, indexFile, err := openFiles(topicName, partition)
	if err != nil {
		return false, response, err
	}
	defer logFile.Close()
	defer indexFile.Close()

	utils.LockFileForWrite(logFile)
	utils.LockFileForWrite(indexFile)
	defer utils.UnlockFile(logFile)
	defer utils.UnlockFile(indexFile)

	startOffsetOfLog, err := logFile.Seek(0, io.SeekEnd)
	if err != nil {
		return false, response, err
	}

	// Get Offset from etcd
	offset, err := GetNextOffset(topicName, partition)
	if err != nil {
		return false, response, err
	}

	timeStamp := utils.GetTimeStamp()
	response = NewMsgProduceResponse{
		Offset:    offset,
		Partition: partition,
		TimeStamp: timeStamp,
	}

	logEntry := fmt.Sprintf("%d--%d--%d--%s\n", response.TimeStamp, response.Partition, response.Offset, msg)
	numOfBytesWritten, err := logFile.WriteString(logEntry)
	if err != nil {
		return false, response, fmt.Errorf("error writing to log file: %w", err)
	}
	endOffsetOfLog := startOffsetOfLog + int64(numOfBytesWritten)

	if err := updateIndex(indexFile, startOffsetOfLog, endOffsetOfLog, timeStamp, offset); err != nil {
		return false, response, err
	}

	return true, response, nil
}

func updateIndex(indexFile *os.File, startOffsetOfLog, endOffsetOfLog, timeStamp int64, offset int) error {
	_, err := indexFile.WriteString(fmt.Sprintf("%d--%d--%d--%d\n", timeStamp, startOffsetOfLog, endOffsetOfLog, offset))
	if err != nil {
		return fmt.Errorf("error writing to index file: %w", err)
	}
	return nil
}

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

func openFiles(topicName string, partition int) (*os.File, *os.File, error) {
	logFilePath := fmt.Sprintf("%s%s/%s-%d.log", constants.FilesDir, topicName, topicName, partition)
	indexFilePath := fmt.Sprintf("%s%s/%s/%s-%d.index", constants.FilesDir, topicName, "index", topicName, partition)

	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening log file: %w", err)
	}

	indexFile, err := os.OpenFile(indexFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logFile.Close()
		return nil, nil, fmt.Errorf("error opening index file: %w", err)
	}

	return logFile, indexFile, nil
}

// GetNextOffset retrieves and increments the offset atomically
func GetNextOffset(topicName string, partition int) (int, error) {
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		return -1, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := fmt.Sprintf("/offsets/%s/%d", topicName, partition)

	// Try to get the current offset
	getResp, err := etcdClient.Get(ctx, key)
	if err != nil {
		return -1, fmt.Errorf("error retrieving offset from etcd: %w", err)
	}

	var currentOffset int

	if len(getResp.Kvs) == 0 {
		// If key doesn't exist, initialize offset to 1
		currentOffset = 1
	} else {
		// Convert existing offset to int
		currentOffset, err = strconv.Atoi(string(getResp.Kvs[0].Value))
		if err != nil {
			return -1, fmt.Errorf("failed to parse offset from etcd: %w", err)
		}
		// Increment the offset
		currentOffset++
	}

	// Store the new incremented offset atomically
	_, err = etcdClient.Put(ctx, key, fmt.Sprint(currentOffset))
	if err != nil {
		return -1, fmt.Errorf("failed to update offset in etcd: %w", err)
	}

	log.Println("Incremented offset to:", currentOffset)
	return currentOffset, nil
}
