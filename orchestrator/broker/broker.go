package broker

import "sync"

type Broker struct {
	Mu                    sync.Mutex
	LastPartition         int
	DistriButionStratergy string
}
