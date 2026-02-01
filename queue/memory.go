package queue

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/xxx-newbee/storage"
)

const (
	MaxRetryCount   = 3
	ShutdownTimeout = 15 * time.Second
)

type queue chan storage.Messager

type MemoryQueue struct {
	queue     *sync.Map
	consumers *sync.Map
	wg        sync.WaitGroup
	exit      chan struct{}
	mtx       sync.RWMutex
	PoolNum   int
}

func NewMemoryQueue(poolNum int) *MemoryQueue {
	return &MemoryQueue{
		queue:     new(sync.Map),
		consumers: new(sync.Map),
		exit:      make(chan struct{}),
		PoolNum:   poolNum,
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
	if message == nil {
		return errors.New("message is nil")
	}
	m.mtx.Lock()
	defer m.mtx.Unlock()

	newMsg := &Message{
		ID:     message.GetID(),
		Stream: message.GetStream(),
		Values: message.GetValues(),
	}

	if newMsg.ID == "" {
		newMsg.SetID(uuid.New().String())
	}

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
		// 非堵塞入队
		select {
		case mq <- msg:
		default:
			// 队列满，可扩展降级策略
		}
	}(newMsg, q)

	return nil
}

func (m *MemoryQueue) Register(name string, f storage.ConsumerFunc) {
	if name == "" || f == nil {
		panic("queue register error")
	}
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.consumers.Store(name, f)
}

func (m *MemoryQueue) consumerLoop(name string, f storage.ConsumerFunc) {
	defer m.wg.Done()

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

	for {
		select {
		case <-m.exit:
			// 处理队列剩余消息，处理完再退出
			for {
				select {
				case msg := <-q:
					if err := f(msg); err != nil {
						continue
					}
				default:
					// 队列无消息，退出
					return
				}
			}
		case msg := <-q:
			// 正常处理消息
			if err := f(msg); err != nil {
				if msg.GetErrorCount() < MaxRetryCount {
					msg.SetErrorCount(msg.GetErrorCount() + 1)
					sec := time.Second * time.Duration(msg.GetErrorCount())
					go func(m storage.Messager, delay time.Duration) {
						time.Sleep(delay)
						select {
						case q <- m:
						default:
							// 队列满了
						}
					}(msg, sec)

				}
			}
		default:
			// 非堵塞处理
			time.Sleep(time.Millisecond * 100)
		}
	}

}

func (m *MemoryQueue) Run() {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	m.consumers.Range(func(key, value interface{}) bool {
		cName, ok := key.(string)
		if !ok {
			return true
		}
		if cf, ok := value.(storage.ConsumerFunc); ok {
			m.wg.Add(1)
			go m.consumerLoop(cName, cf)
		}
		return true
	})
}

func (m *MemoryQueue) Shutdown() {
	close(m.exit)
	// 等待消费协程处理剩余消息，15s超时
	done := make(chan struct{})

	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(ShutdownTimeout):
	}
	// 关闭所有消息队列
	m.queue.Range(func(key, value interface{}) bool {
		if q, ok := value.(queue); ok {
			close(q)
		}
		return true
	})

}
