package producer

type NewMsgProduceResponse struct {
	Offset    int
	Partition int
	TimeStamp int64
}

type Message struct {
	Offset int         `json:"offset"`
	Key    string      `json:"key"`
	Value  interface{} `json:"message"`
}
