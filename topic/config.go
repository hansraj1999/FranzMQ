package topic

type Config struct {
	Compression        string
	DataType           string
	Replicas           int    // TODO: use this to replicate
	NumOfPartition     int    // 0
	PartitionStratergy string // round robin, hash based
}
