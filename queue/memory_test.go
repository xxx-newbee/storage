package queue

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xxx-newbee/storage"
)

// 模拟消费函数：统计成功/失败次数
type mockConsumer struct {
	successCount int32
	failCount    int32
	retryCount   int32
}

func (m *mockConsumer) consume(msg storage.Messager) error {
	// 模拟前两次失败，第三次成功
	if msg.GetErrorCount() < 2 {
		atomic.AddInt32(&m.failCount, 1)
		atomic.AddInt32(&m.retryCount, 1)
		return errors.New("模拟消费失败")
	}
	atomic.AddInt32(&m.successCount, 1)
	return nil
}

// 测试基本流程：注册→入队→运行→关闭
func TestMemoryQueue_BasicFlow(t *testing.T) {
	// 1. 初始化队列
	mq := NewMemoryQueue(10)
	if mq.String() != "memory" {
		t.Error("队列名称错误")
	}

	// 2. 注册消费者
	consumer := &mockConsumer{}
	streamName := "test-stream"
	mq.Register(streamName, consumer.consume)

	// 3. 入队1条测试消息
	testMsg := &Message{
		Stream: streamName,
		Values: map[string]interface{}{"key": "value"},
	}
	err := mq.Append(testMsg)
	if err != nil {
		t.Fatalf("消息入队失败: %v", err)
	}

	// 4. 启动消费者
	mq.Run()

	// 5. 等待消费完成（包含重试）
	time.Sleep(5 * time.Second)

	// 6. 关闭队列
	mq.Shutdown()

	// 7. 验证结果
	if consumer.successCount != 1 {
		t.Errorf("成功消费次数错误，期望1，实际%d", consumer.successCount)
	}
	if consumer.failCount != 2 {
		t.Errorf("失败消费次数错误，期望2，实际%d", consumer.failCount)
	}
	if consumer.retryCount != 2 {
		t.Errorf("重试次数错误，期望2，实际%d", consumer.retryCount)
	}
}

// 测试Shutdown时剩余消息处理
func TestMemoryQueue_ShutdownWithRemainingMsg(t *testing.T) {
	mq := NewMemoryQueue(10)
	streamName := "shutdown-test"

	// 1. 注册消费者（直接消费成功）
	success := make(chan bool, 1)
	mq.Register(streamName, func(msg storage.Messager) error {
		success <- true
		return nil
	})

	// 2. 入队1条消息（不启动消费者）
	mq.Append(&Message{Stream: streamName})

	// 3. 关闭队列（触发剩余消息处理）
	mq.Shutdown()

	// 4. 验证消息被处理
	select {
	case <-success:
	case <-time.After(2 * time.Second):
		t.Error("Shutdown时剩余消息未处理")
	}
}

// 测试队列满时的非阻塞入队
func TestMemoryQueue_QueueFull(t *testing.T) {
	// 初始化缓冲为1的队列
	mq := NewMemoryQueue(1)
	streamName := "full-test"

	// 1. 连续入队2条消息（超出缓冲）
	msg1 := &Message{Stream: streamName}
	msg2 := &Message{Stream: streamName}
	mq.Append(msg1)
	mq.Append(msg2)

	// 2. 启动消费者消费第一条消息
	consumed := make(chan bool, 1)
	mq.Register(streamName, func(msg storage.Messager) error {
		consumed <- true
		return nil
	})
	mq.Run()
	time.Sleep(1 * time.Second)
	mq.Shutdown()

	// 3. 验证第一条被消费，第二条入队失败（日志可查）
	select {
	case <-consumed:
	default:
		t.Error("第一条消息未被消费")
	}
}
