package producer

import (
	"FranzMQ/constants"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	queueLock   sync.Mutex
	logQueues   = make(map[string]map[int]chan LogEntry) // Topic → Partition → Log Queue
	indexQueues = make(map[string]map[int]chan LogEntry) // Topic → Partition → Index Queue
)

type LogEntry struct {
	Ctx   context.Context
	Entry string
}

// Initialize queues for a given topic with M partitions
func InitQueues(topicName string, partitions int) {
	_, span := constants.Tracer.Start(context.Background(), "InitQueues")
	defer span.End()

	queueLock.Lock()
	defer queueLock.Unlock()

	if _, exists := logQueues[topicName]; !exists {
		logQueues[topicName] = make(map[int]chan LogEntry)
		indexQueues[topicName] = make(map[int]chan LogEntry)
	}

	for i := 0; i < partitions; i++ {
		logQueues[topicName][i] = make(chan LogEntry, 10000)
		indexQueues[topicName][i] = make(chan LogEntry, 10000)

		go processLogQueue(topicName, i)
	}
}

// Process log queues for all topics and partitions
func processLogQueue(topic string, partition int) {
	for logEntry := range logQueues[topic][partition] {
		ctx, span := constants.Tracer.Start(logEntry.Ctx, "processLogQueue")
		defer span.End()

		offsetKey := topic + "-" + strconv.Itoa(partition)
		offset := constants.OffsetMap.INCR(ctx, offsetKey)

		timeStamp := time.Now().UnixNano()
		updatedLogEntry := fmt.Sprintf("%d--%d--%d--%s\n", timeStamp, partition, offset, logEntry.Entry)

		log.Println("Writing to log file with offset:", offset)
		appendToFile(ctx, getLogFilePath(topic, partition), updatedLogEntry)

		endOffset := constants.LogSizeMap.INCRBY(ctx, offsetKey, len(updatedLogEntry))
		indexEntry := fmt.Sprintf("%d--%d--%d--%d\n", timeStamp, endOffset-len(updatedLogEntry), endOffset, offset)

		// ✅ Directly append index entry instead of using indexQueues
		appendToFile(ctx, getIndexFilePath(topic, partition), indexEntry)
	}
}

// Get log file path
func getLogFilePath(topic string, partition int) string {
	return fmt.Sprintf("./files/topics/%s/%s-%d.log", topic, topic, partition)
}

// Get index file path
func getIndexFilePath(topic string, partition int) string {
	return fmt.Sprintf("./files/topics/%s/index/%s-%d.index", topic, topic, partition)
}

// Append data to file
func appendToFile(ctx context.Context, filePath, entry string) {
	ctx, span := constants.Tracer.Start(ctx, "appendToFile")
	defer span.End()

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	log.Println("appendToFile: Writing to file:", filePath)
	if _, err := file.WriteString(entry); err != nil {
		log.Println("Error writing to file:", err)
	}
}
