package topic

import (
	"FranzMQ/constants"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
)

func CreateAtTopic(name string, config Config) (bool, error) {
	exist := fileExists(name)
	log.Println("starting to create a topic with name:", name, "config", config)
	if exist {
		return false, fmt.Errorf("topic already exist")
	}
	if config.NumOfPartition < 1 {
		return false, fmt.Errorf("minimum number of partition is 1, it should be minimum above 0")
	}
	err := os.MkdirAll(constants.FilesDir+name, 0755) // 0755 sets permissions (read/write/execute)
	if err != nil {
		log.Println(err)
		return false, fmt.Errorf("error while creating topic path")
	}
	log.Printf("Starting creating files")
	for i := range config.NumOfPartition {
		file, err := os.Create(constants.FilesDir + name + "/" + name + "-" + strconv.Itoa(i) + ".log")
		if err != nil {
			log.Println("Error creating topic file:", err)
			return false, err
		}
		OffsetFile, OffsetFileErr := os.Create(constants.FilesDir + name + "/" + name + "-" + strconv.Itoa(i) + ".json")
		offsetData := map[string]int{"Offset": 0}
		jsonData, err := json.MarshalIndent(offsetData, "", "  ")
		OffsetFile.Write(jsonData)
		if err != nil || OffsetFileErr != nil {
			log.Println("Error creating meta file:", err)
			return false, err
		}
		defer OffsetFile.Close()
		defer file.Close()
		log.Println("File created successfully" + name + "-" + strconv.Itoa(i))
	}

	jsonData, err := json.MarshalIndent(config, "", "  ") // Pretty print
	if err != nil {
		log.Println("Error encoding JSON:", err)
		return false, fmt.Errorf("error while converting config into json")
	}

	file, err := os.Create(constants.FilesDir + name + "/" + name + ".json")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return false, fmt.Errorf("error while converting config into json")
	}
	defer file.Close() // Ensure file closes after writing

	_, err = file.Write(jsonData)
	if err != nil {
		fmt.Println("Error writing JSON to file:", err)
		return false, fmt.Errorf("error writing JSON to file")
	}

	return true, nil
}

func fileExists(name string) bool {
	_, err := os.Stat(constants.FilesDir + name)
	if err != nil {
		if os.IsNotExist(err) {
			return false // File does not exist
		}
		log.Println("Error checking file:", err) // Other errors (e.g., permission issues)
		return false
	}
	return true // File exists
}
