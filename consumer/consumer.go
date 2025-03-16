package consumer

func Consume(topic string, consumerGroupID string) {
	// checks if the topic exists
	// checks if this consumer group is already subscribed to the topic check if another partition is already being consumed and if spare partitions are available then allow
	// new for same topic rebalance all others
}
