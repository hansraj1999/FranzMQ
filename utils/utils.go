package utils

import (
	"FranzMQ/constants"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	etcdClient *clientv3.Client
	once       sync.Once
)

var (
	fileCache   sync.Map // Stores file existence: key -> exists (bool)
	cacheExpiry = 60 * time.Second
)

// FileCacheEntry represents a cached file check result
type FileCacheEntry struct {
	exists     bool
	lastUpdate time.Time
}

func FileExists(ctx context.Context, name string) bool {
	_, span := constants.Tracer.Start(ctx, "FileExists")
	defer span.End()

	filePath := constants.FilesDir + name
	now := time.Now()

	// Check cache first
	if cached, ok := fileCache.Load(filePath); ok {
		entry := cached.(FileCacheEntry)
		if now.Sub(entry.lastUpdate) < cacheExpiry {
			return entry.exists
		}
	}

	// Perform actual file check
	_, err := os.Stat(filePath)
	exists := err == nil || !os.IsNotExist(err)

	// Log errors (but don't cache them)
	if err != nil && !os.IsNotExist(err) {
		fmt.Println("Error checking file:", err)
	}

	// Cache result
	fileCache.Store(filePath, FileCacheEntry{exists: exists, lastUpdate: now})

	return exists
}

func HashKeyToPartition(ctx context.Context, key string, numPartitions int) int {
	_, span := constants.Tracer.Start(ctx, "HashKeyToPartition")
	defer span.End()
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}

func MurmurHashKeyToPartition(ctx context.Context, key string, numPartitions int) int {
	_, span := constants.Tracer.Start(ctx, "MurmurHashKeyToPartition")
	defer span.End()
	h := murmur3.New32()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}

func GetTimeStamp(ctx context.Context) int64 {
	_, span := constants.Tracer.Start(ctx, "GetTimeStamp")
	defer span.End()
	return time.Now().Unix()
}

// Generic function to convert any struct to JSON string
func StructToJSON(ctx context.Context, v interface{}) (string, error) {
	_, span := constants.Tracer.Start(ctx, "StructToJSON")
	defer span.End()
	jsonData, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}
