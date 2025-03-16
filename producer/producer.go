package producer

import (
	"FranzMQ/constants"
	"FranzMQ/utils"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

type Config struct {
	NumOfPartition int `json:"NumOfPartition"`
}

// have a stratergy class to determins the stratergy
// have a stratergy class to determine the deserialization
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

	logFile, metaFile, err := openFiles(topicName, partition)
	if err != nil {
		return false, response, err
	}
	defer logFile.Close()
	defer metaFile.Close()

	offset, err := updateOffset(metaFile)
	if err != nil {
		return false, response, err
	}

	response = NewMsgProduceResponse{
		Offset:    offset,
		Partition: partition,
		TimeStamp: utils.GetTimeStamp(),
	}

	logEntry := fmt.Sprintf("%d--%d--%d--%s\n", response.TimeStamp, response.Partition, response.Offset, msg)
	if _, err := logFile.WriteString(logEntry); err != nil {
		return false, response, fmt.Errorf("error writing to log file: %w", err)
	}

	return true, response, nil
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
	metaFilePath := fmt.Sprintf("%s%s/%s-%d.json", constants.FilesDir, topicName, topicName, partition)

	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening log file: %w", err)
	}

	metaFile, err := os.OpenFile(metaFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		logFile.Close()
		return nil, nil, fmt.Errorf("error opening meta file: %w", err)
	}

	return logFile, metaFile, nil
}

func updateOffset(metaFile *os.File) (int, error) {
	var meta struct {
		Offset int `json:"Offset"`
	}

	metaData, err := io.ReadAll(metaFile)
	if err != nil {
		return -1, fmt.Errorf("error reading meta file: %w", err)
	}

	if len(metaData) > 0 {
		if err := json.Unmarshal(metaData, &meta); err != nil {
			return -1, fmt.Errorf("error unmarshaling meta file: %w", err)
		}
	}

	meta.Offset++

	metaFile.Truncate(0)
	metaFile.Seek(0, 0)
	jsonData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return -1, fmt.Errorf("error encoding meta file: %w", err)
	}
	metaFile.Write(jsonData)

	return meta.Offset, nil
}
