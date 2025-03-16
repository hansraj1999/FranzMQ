package orchestrator

import (
	"FranzMQ/orchestrator/broker"
)

type Orchestrator struct {
	topic_name_to_broker map[string]broker.Broker
}
