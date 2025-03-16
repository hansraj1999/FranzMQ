package utils

import (
	"FranzMQ/constants"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"time"

	"syscall"

	"github.com/spaolacci/murmur3"
)

func FileExists(name string) bool {
	_, err := os.Stat(constants.FilesDir + name)
	if err != nil {
		if os.IsNotExist(err) {
			return false // File does not exist
		}
		fmt.Println("Error checking file:", err) // Other errors (e.g., permission issues)
		return false
	}
	return true // File exists
}

func HashKeyToPartition(key string, numPartitions int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}

func MurmurHashKeyToPartition(key string, numPartitions int) int {
	h := murmur3.New32()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}

func LockFileForWrite(file *os.File) error {
	log.Println("Locking file for write", file.Name(), file.Fd(), file)
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
}

// unlockFile releases the lock
func UnlockFile(file *os.File) error {
	log.Println("UnLocking file for write", file.Name(), file.Fd(), file)
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

func GetTimeStamp() int64 {
	return time.Now().Unix()

}
