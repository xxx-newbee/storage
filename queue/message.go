package queue

import (
	"sync"

	"github.com/xxx-newbee/storage"
)

type Message struct {
	ID         string
	Stream     string
	Values     map[string]interface{}
	ErrorCount int
	mtx        sync.Mutex
}

func (m *Message) SetID(id string) {
	m.ID = id
}

func (m *Message) SetStream(stream string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.Stream = stream
}

func (m *Message) SetValues(v map[string]interface{}) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.Values == nil {
		m.Values = make(map[string]interface{})
	}
	m.Values = v
}

func (m *Message) GetID() string {
	return m.ID
}

func (m *Message) GetStream() string {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.Stream
}

func (m *Message) GetValues() map[string]interface{} {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return m.Values
}

func (m *Message) GetPrefix() string {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.Values == nil {
		return ""
	}
	v := m.Values[storage.PrefixKey]
	prefix, _ := v.(string)
	return prefix
}

func (m *Message) SetPrefix(prefix string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.Values == nil {
		m.Values = make(map[string]interface{})
	}
	m.Values[storage.PrefixKey] = prefix
}

func (m *Message) SetErrorCount(count int) {
	m.ErrorCount = count
}

func (m *Message) GetErrorCount() int {
	return m.ErrorCount
}
