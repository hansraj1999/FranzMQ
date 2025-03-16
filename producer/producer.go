package producer

import (
	"FranzMQ/constants"
	"FranzMQ/utils"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Config represents topic configuration
type Config struct {
	NumOfPartition int `json:"NumOfPartition"`
}

// ProduceMessage writes a message with a distributed offset mechanism
func ProduceMessage(topicName, key, msg string) (bool, NewMsgProduceResponse, error) {
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
	// utils.LockFileForWrite(logFile)

	// Write log entry
	logEntry := fmt.Sprintf("%d--%d--%d--%d--%s\n", time.Now().UnixNano(), partition, key, offset, msg)
	startOffset, endOffset, err := GetAndIncrementLogMessageOffset(topicName, partition, len(logEntry))
	if _, err := logFile.WriteString(logEntry); err != nil {
		return false, NewMsgProduceResponse{}, fmt.Errorf("error writing log: %w", err)
	}

	// Update index file with correct start and end offsets
	indexEntry := fmt.Sprintf("%d--%d--%d--%d\n", time.Now().UnixNano(), startOffset, endOffset, offset)
	if _, err := indexFile.WriteString(indexEntry); err != nil {
		return false, NewMsgProduceResponse{}, fmt.Errorf("error writing index: %w", err)
	}
	// utils.UnlockFile(logFile)
	return true, NewMsgProduceResponse{Offset: offset, Partition: partition, TimeStamp: time.Now().UnixNano()}, nil
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	key := fmt.Sprintf("/offsets/%s/%d/start/offset", topicName, partition)

	// Atomic transaction: Read & Update offset in one go
	resp, err := etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), ">", 0)). // Check if key exists
		Then(clientv3.OpGet(key)).                                  // Fetch current offset
		Else(clientv3.OpPut(key, "0")).                             // Initialize offset to 0
		Commit()

	if err != nil {
		return -1, -1, fmt.Errorf("etcd transaction error: %w", err)
	}

	var currentOffset int
	if resp.Succeeded { // Key existed, extract current offset
		value := resp.Responses[0].GetResponseRange().Kvs
		if len(value) == 0 {
			return -1, -1, fmt.Errorf("unexpected empty offset response")
		}
		currentOffset, err = strconv.Atoi(string(value[0].Value))
		if err != nil {
			return -1, -1, fmt.Errorf("error parsing offset: %w", err)
		}
	} else {
		// First time: Initialize to 0 manually
		currentOffset = 0
	}

	nextOffset := currentOffset + size // Compute new offset

	// ✅ Perform the atomic increment within the same transaction
	updateResp, err := etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", fmt.Sprint(currentOffset))). // Ensure atomic update
		Then(clientv3.OpPut(key, fmt.Sprint(nextOffset))).                         // Update offset in the same txn
		Commit()

	if err != nil {
		return -1, -1, fmt.Errorf("failed to update offset: %w", err)
	}

	if !updateResp.Succeeded {
		return -1, -1, fmt.Errorf("concurrent modification detected, retrying may be needed")
	}

	log.Println("Assigned new offset range:", currentOffset, "to", nextOffset)
	return currentOffset, nextOffset, nil
}
