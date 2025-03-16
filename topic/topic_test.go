package topic

import (
	"FranzMQ/constants"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestCreateAtTopic verifies that topics are correctly created and deleted.
func TestCreateAtTopic(t *testing.T) {
	topicName := "test_topic"
	baseDir := constants.FilesDir // Ensure this is correctly set

	// Ensure the test directory is clean before starting
	topicPath := filepath.Join(baseDir, topicName)
	_ = os.RemoveAll(topicPath)

	config := Config{NumOfPartition: 3}

	// 1️⃣ Test: Create a new topic successfully
	success, err := CreateAtTopic(topicName, config)
	if !success || err != nil {
		t.Fatalf("Expected topic creation to succeed, got error: %v", err)
	}

	// 2️⃣ Verify topic directory exists
	if _, err := os.Stat(topicPath); os.IsNotExist(err) {
		t.Fatalf("Expected topic directory %s to be created, but it does not exist", topicPath)
	}

	// 3️⃣ Verify partition files exist
	for i := 0; i < config.NumOfPartition; i++ {
		logFile := filepath.Join(topicPath, topicName+"-"+strconv.Itoa(i)+".log")
		metaFile := filepath.Join(topicPath, topicName+"-"+strconv.Itoa(i)+".json")

		if _, err := os.Stat(logFile); os.IsNotExist(err) {
			t.Errorf("Expected log file %s to be created, but it does not exist", logFile)
		}
		if _, err := os.Stat(metaFile); os.IsNotExist(err) {
			t.Errorf("Expected meta file %s to be created, but it does not exist", metaFile)
		}
	}

	// 4️⃣ Test: Try creating the same topic again (should fail)
	success, err = CreateAtTopic(topicName, config)
	if success || err == nil {
		t.Errorf("Expected topic creation to fail for existing topic, but it succeeded")
	}

	// 5️⃣ Clean up: Delete the created topic directory
	err = os.RemoveAll(topicPath)
	if err != nil {
		t.Fatalf("Failed to clean up test topic directory: %v", err)
	}

	// 6️⃣ Verify deletion
	if _, err := os.Stat(topicPath); !os.IsNotExist(err) {
		t.Errorf("Expected topic directory %s to be deleted, but it still exists", topicPath)
	}
}
