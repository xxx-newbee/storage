package queue

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/xxx-newbee/storage"
)

type queue chan storage.Messager

type MemoryQueue struct {
	queue   *sync.Map
	wg      sync.WaitGroup
	mtx     sync.RWMutex
	PoolNum int
}

func NewMemoryQueue(poolNum int) *MemoryQueue {
	return &MemoryQueue{
		queue:   new(sync.Map),
		PoolNum: poolNum,
	}
}

func (m *MemoryQueue) String() string {
	return "memory"
}

func (m *MemoryQueue) makeQueue() queue {
	if m.PoolNum <= 0 {
		return make(queue)
	}
	return make(queue, m.PoolNum)
}

func (m *MemoryQueue) Append(message storage.Messager) error {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	newMsg := new(Message)
	newMsg.SetID(message.GetID())
	newMsg.SetStream(message.GetStream())
	newMsg.SetValues(message.GetValues())

	v, ok := m.queue.Load(message.GetStream())
	if !ok {
		v = m.makeQueue()
		m.queue.Store(message.GetStream(), v)
	}

	var q queue
	switch vv := v.(type) {
	case queue:
		q = vv
	default:
		q = m.makeQueue()
		m.queue.Store(message.GetStream(), q)
	}

	go func(msg storage.Messager, mq queue) {
		msg.SetID(uuid.New().String())
		mq <- msg
	}(newMsg, q)

	return nil
}

func (m *MemoryQueue) Register(name string, f storage.ConsumerFunc) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	v, ok := m.queue.Load(name)
	if !ok {
		v = m.makeQueue()
		m.queue.Store(name, v)
	}

	var q queue
	switch vv := v.(type) {
	case queue:
		q = vv
	default:
		q = m.makeQueue()
		m.queue.Store(name, q)
	}

	go func(out queue, mf storage.ConsumerFunc) {
		for msg := range out { // range out等价于 <-out
			if err := mf(msg); err != nil {
				if msg.GetErrorCount() < 3 {
					msg.SetErrorCount(msg.GetErrorCount() + 1)
					sec := time.Second * time.Duration(msg.GetErrorCount())
					time.Sleep(sec)
					out <- msg
				}
			}
		}
	}(q, f)
}

func (m *MemoryQueue) Run() {
	m.wg.Add(1)
	m.wg.Wait()
}

func (m *MemoryQueue) Shutdown() {
	m.wg.Done()
}
