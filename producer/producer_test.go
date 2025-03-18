package producer

import (
	"FranzMQ/constants"
	"FranzMQ/metrics"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

var tp, _ = metrics.StartTracing()

func setupTestTopic(topic string, numOfPartition int) {
	os.MkdirAll(constants.FilesDir+topic, 0755)
	os.MkdirAll(constants.FilesDir+topic+"/"+"meta", 0755)
	os.MkdirAll(constants.FilesDir+topic+"/"+"index", 0755)
	config := map[string]interface{}{"NumOfPartition": numOfPartition}
	configData, _ := json.Marshal(config)
	ioutil.WriteFile(constants.FilesDir+topic+"/"+topic+".json", configData, 0644)
	for i := 0; i < numOfPartition; i++ {
		ioutil.WriteFile(constants.FilesDir+topic+"/"+topic+"-"+strconv.Itoa(i)+".log", []byte(""), 0644)
		ioutil.WriteFile(constants.FilesDir+topic+"/"+"meta/"+topic+"-"+strconv.Itoa(i)+".json", []byte("{\"Offset\": 0}"), 0644)
	}
}

func teardownTestTopic(topic string) {
	os.RemoveAll(constants.FilesDir + topic)
}
func TestProduceMessage_Success(t *testing.T) {
	topic := "test_topic"
	setupTestTopic(topic, 3)
	defer teardownTestTopic(topic)
	ctx, span := constants.Tracer.Start(context.Background(), "TestProduceMessage_Success POST")
	defer span.End()
	success, response, err := ProduceMessage(ctx, topic, "key1", "Hello Kafka")

	if !success || err != nil {
		t.Errorf("Expected success but got error: %v", err)
	}
	if response.Offset < 0 {
		t.Errorf("Expected offset >= 0 but got %d", response.Offset)
	}
	if response.Partition < 0 || response.Partition >= 3 {
		t.Errorf("Partition out of range: %d", response.Partition)
	}
}

func TestProduceMessage_TopicDoesNotExist(t *testing.T) {
	topic := "non_existing_topic"
	ctx, span := constants.Tracer.Start(context.Background(), "TestProduceMessage_Success POST")
	defer span.End()
	success, _, err := ProduceMessage(ctx, topic, "key1", "Hello Kafka")

	if success || err == nil {
		t.Errorf("Expected failure for non-existing topic but got success")
	}
}

func TestProduceMessage_Partitioning(t *testing.T) {
	topic := "partition_test"
	setupTestTopic(topic, 5)
	defer teardownTestTopic(topic)
	ctx, span := constants.Tracer.Start(context.Background(), "TestProduceMessage_Success POST")
	defer span.End()
	success1, response1, _ := ProduceMessage(ctx, topic, "keyA", "Message 1")
	success2, response2, _ := ProduceMessage(ctx, topic, "keyA", "Message 2")

	if !success1 || !success2 {
		t.Errorf("Expected both messages to be produced successfully")
	}
	if response1.Partition != response2.Partition {
		t.Errorf("Expected same partition for same key but got %d and %d", response1.Partition, response2.Partition)
	}
}

func TestProduceMessage_OffsetIncrement(t *testing.T) {
	ctx, span := constants.Tracer.Start(context.Background(), "TestProduceMessage_Success POST")
	defer span.End()
	topic := "offset_test"
	setupTestTopic(topic, 1)
	defer teardownTestTopic(topic)

	_, response1, _ := ProduceMessage(ctx, topic, "key1", "Msg 1")
	_, response2, _ := ProduceMessage(ctx, topic, "key1", "Msg 2")

	if response2.Offset != response1.Offset+1 {
		t.Errorf("Expected offset to increment sequentially but got %d and %d", response1.Offset, response2.Offset)
	}
}
