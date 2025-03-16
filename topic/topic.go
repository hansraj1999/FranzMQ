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
	err := createTopicDirectories(name)
	if err != nil {
		log.Println("Error creating topic directories:", err)
		return false, err
	}
	log.Printf("Starting creating files")
	_, createFilesErr := createFiles(config, name)
	if createFilesErr != nil {
		log.Println("Error creating topic files:", createFilesErr)
		return false, createFilesErr
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
func createFiles(config Config, name string) (bool, error) {
	for i := range config.NumOfPartition {
		file, err := os.Create(constants.FilesDir + name + "/" + name + "-" + strconv.Itoa(i) + ".log")
		if err != nil {
			log.Println("Error creating topic file:", err)
			return false, err
		}
		OffsetFile, OffsetFileErr := os.Create(constants.FilesDir + name + "/" + "meta" + "/" + name + "-" + strconv.Itoa(i) + ".json")
		offsetData := map[string]int{"Offset": 0}
		jsonData, err := json.MarshalIndent(offsetData, "", "  ")
		OffsetFile.Write(jsonData)
		if err != nil || OffsetFileErr != nil {
			log.Println("Error creating meta file:", err)
			return false, err
		}
		IndexFile, IndexFileErr := os.Create(constants.FilesDir + name + "/" + "index" + "/" + name + "-" + strconv.Itoa(i) + ".index")
		_, er := IndexFile.WriteString("timestamp--start--end--offset\n")
		if er != nil || IndexFileErr != nil {
			log.Println("Error creating meta file:", err)
			return false, err
		}
		defer IndexFile.Close()
		defer OffsetFile.Close()
		defer file.Close()
		log.Println("File created successfully" + name + "-" + strconv.Itoa(i))

	}
	return true, nil
}

func createTopicDirectories(name string) error {
	paths := []string{
		constants.FilesDir + name,
		constants.FilesDir + name + "/meta",
		constants.FilesDir + name + "/index",
	}

	for _, path := range paths {
		if err := os.MkdirAll(path, 0755); err != nil {
			log.Println(err)
			return fmt.Errorf("error creating directory: %s", path)
		}
	}
	return nil
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
