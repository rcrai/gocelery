package gocelery

import "sync/atomic"

const (
	DefaultQueueName = "celery"
)

type QueueIterator struct {
	queues []string
	cQueue *int64
}

func NewQueueIterator(queues ...string) *QueueIterator {
	if len(queues) == 0 {
		queues = append(queues, DefaultQueueName)
	}
	base := int64(0)
	return &QueueIterator{
		queues: queues,
		cQueue: &base,
	}
}

// next next cnt
func (qi *QueueIterator) next() int64 {
	return atomic.AddInt64(qi.cQueue, 1)
}

func (qi *QueueIterator) DefaultQueueName() string {
	if len(qi.queues) > 0 {
		return qi.queues[0]
	} else {
		return DefaultQueueName
	}
}

func (qi *QueueIterator) NextQueueName() string {
	queues := qi.queues
	return queues[int(qi.next())%(qi.Length())]
}

func (qi *QueueIterator) ListQueues() []string {
	return append([]string{}, qi.queues...)
}

func (cb *QueueIterator) Length() int {
	return len(cb.queues)
}
