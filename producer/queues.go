package producer

import (
	"FranzMQ/constants"
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	queueLock              sync.Mutex
	logQueues              = make(map[string]map[int]chan LogEntry) // Topic → Partition → Log Queue
	GlobalLogWriterQueue   = make(chan LogWrite, 10000)             // Global queue for log writes
	GlobalIndexWriterQueue = make(chan LogWrite, 10000)             // Global queue for index writes
)

type LogEntry struct {
	Ctx      context.Context
	Entry    string
	Callback chan int // Callback channel for offset
}
type LogWrite struct {
	Ctx      context.Context
	FilePath string
	Entry    string
}

// Initialize queues for a given topic with M partitions
func InitQueues(topicName string, partitions int) {
	_, span := constants.Tracer.Start(context.Background(), "InitQueues")
	defer span.End()

	queueLock.Lock()
	defer queueLock.Unlock()

	if _, exists := logQueues[topicName]; !exists {
		logQueues[topicName] = make(map[int]chan LogEntry)
	}

	for i := 0; i < partitions; i++ {
		logQueues[topicName][i] = make(chan LogEntry, 10000)
		go processLogQueue(topicName, i)
	}

	// Start global writer threads
}

// Process log queue and push entries to writer queues
func processLogQueue(topic string, partition int) {
	for logEntry := range logQueues[topic][partition] {
		ctx, span := constants.Tracer.Start(logEntry.Ctx, "processLogQueue")
		defer span.End()

		offsetKey := topic + "-" + strconv.Itoa(partition)
		offset := constants.OffsetMap.INCR(ctx, offsetKey)
		if logEntry.Callback != nil {
			logEntry.Callback <- offset
		}
		timeStamp := time.Now().UnixNano()
		logEntryStr := fmt.Sprintf("%d--%d--%d--%s\n", timeStamp, partition, offset, logEntry.Entry)

		log.Println("Queueing log entry with offset:", offset)
		GlobalLogWriterQueue <- LogWrite{Ctx: ctx, FilePath: getLogFilePath(topic, partition), Entry: logEntryStr}

		endOffset := constants.LogSizeMap.INCRBY(ctx, offsetKey, len(logEntryStr))
		indexEntry := fmt.Sprintf("%d--%d--%d--%d\n", timeStamp, endOffset-len(logEntryStr), endOffset, offset)
		GlobalIndexWriterQueue <- LogWrite{Ctx: ctx, FilePath: getIndexFilePath(topic, partition), Entry: indexEntry}
	}
}

// Global writer thread for logs & indexes
func GlobalWriterThread(writerQueue chan LogWrite) {
	fileMap := make(map[string]*bufio.Writer)
	fileHandles := make(map[string]*os.File)

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	batch := make(map[string][]LogWrite) // Group writes by file
	for {
		select {
		case logWrite := <-writerQueue:
			batch[logWrite.FilePath] = append(batch[logWrite.FilePath], logWrite)
			if len(batch[logWrite.FilePath]) >= 200 {
				flushBuffer(batch, fileMap, fileHandles)
				batch = make(map[string][]LogWrite) // Clear batch after flushing
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushBuffer(batch, fileMap, fileHandles)
				batch = make(map[string][]LogWrite) // Clear batch after flushing
			}
		}
	}
}

// Flush batched writes to corresponding files
func flushBuffer(batch map[string][]LogWrite, fileMap map[string]*bufio.Writer, fileHandles map[string]*os.File) {
	for filePath, entries := range batch {
		ctx := entries[0].Ctx
		_, span := constants.Tracer.Start(ctx, "flushBuffer")
		defer span.End()

		if _, exists := fileMap[filePath]; !exists {
			file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				log.Println("Error opening file:", err)
				continue
			}
			fileHandles[filePath] = file
			fileMap[filePath] = bufio.NewWriter(file)
		}

		writer := fileMap[filePath]
		for _, entry := range entries {
			_, err := writer.WriteString(entry.Entry)
			if err != nil {
				log.Println("Error writing to buffer:", err)
				return
			}
		}
		if err := writer.Flush(); err != nil {
			log.Println("Error flushing buffer:", err)
		}
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
