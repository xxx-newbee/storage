package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xxx-newbee/storage"
)

// 全局测试用Redis客户端（用于清理数据）
var testRdb = redis.NewClient(&redis.Options{
	Addr:     "0.0.0.0:6379",
	Password: "",
	DB:       15, // 测试专用DB
})

// 测试Append方法的核心场景
func TestRedisQueue_Append(t *testing.T) {
	ctx := context.Background()
	// 测试前清理测试DB的所有数据
	defer func() {
		if err := testRdb.FlushDB(ctx).Err(); err != nil {
			t.Fatalf("清理测试Redis DB失败: %v", err)
		}
		testRdb.Close()
	}()

	var localRdb = redis.NewClient(&redis.Options{
		Addr:     "0.0.0.0:6379",
		Password: "",
		DB:       15,
	})

	// 初始化测试用队列实例
	testQueue := NewRedisQueue(localRdb, "test_prefix", 3)
	defer testQueue.Shutdown()

	// 定义测试用例
	tests := []struct {
		name        string                                   // 测试用例名称
		msg         storage.Messager                         // 待追加的消息
		wantErr     bool                                     // 在执行Append的时候是否期望返回错误
		checkFunc   func(t *testing.T, msg storage.Messager) // 自定义校验逻辑
		prepareFunc func()                                   // 测试前准备（如关闭队列）
	}{
		// 场景1：正常追加消息（完整属性）
		{
			name: "正常追加-完整属性",
			msg: &Message{
				ID:         "test_msg_001",
				Stream:     "test_01",
				Values:     map[string]interface{}{"_host": "test_01_prefix", "key1": "value1", "num": 123},
				ErrorCount: 0,
			},
			wantErr: false,
			checkFunc: func(t *testing.T, msg storage.Messager) {
				// 校验Redis队列中是否存在该消息
				queueKey := fmt.Sprintf("%s:%s", msg.GetPrefix(), msg.GetStream())
				msgList, err := testRdb.LRange(ctx, queueKey, 0, -1).Result()
				if err != nil {
					t.Fatalf("读取Redis队列失败: %v", err)
				}
				if len(msgList) == 0 {
					t.Error("队列中无消息，追加失败")
					return
				}

				// 反序列化消息，校验内容是否一致
				var actualMsg Message
				if err := json.Unmarshal([]byte(msgList[0]), &actualMsg); err != nil {
					t.Fatalf("反序列化队列消息失败: %v", err)
				}
				if actualMsg.GetID() != msg.GetID() {
					t.Errorf("消息ID不匹配，期望: %s, 实际: %s", msg.GetID(), actualMsg.GetID())
				}
				if actualMsg.GetStream() != msg.GetStream() {
					t.Errorf("消息Stream不匹配，期望: %s, 实际: %s", msg.GetStream(), actualMsg.GetStream())
				}
				if actualMsg.GetValues()["key1"] != msg.GetValues()["key1"] {
					t.Errorf("消息Values不匹配，期望: %v, 实际: %v", msg.GetValues()["key1"], actualMsg.GetValues()["key1"])
				}
			},
		},
		// 场景2：消息未设置ID，验证自动生成ID
		{
			name: "正常追加-自动生成ID",
			msg: &Message{
				Stream:     "test_02",
				Values:     map[string]interface{}{"_host": "test_02_prefix", "key2": "value2"},
				ErrorCount: 0,
			},
			wantErr: false,
			checkFunc: func(t *testing.T, msg storage.Messager) {
				queueKey := fmt.Sprintf("%s:%s", msg.GetPrefix(), msg.GetStream())
				msgList, err := testRdb.LRange(ctx, queueKey, 0, -1).Result()
				if err != nil || len(msgList) == 0 {
					t.Fatalf("读取队列失败或无消息: %v", err)
				}

				var actualMsg Message
				_ = json.Unmarshal([]byte(msgList[0]), &actualMsg)
				if actualMsg.GetID() == "" {
					t.Error("消息ID未自动生成，为空")
				}
				// 校验ID格式（msg_时间戳_随机数）
				if len(actualMsg.GetID()) < 10 || actualMsg.GetID()[:4] != "msg_" {
					t.Errorf("自动生成的ID格式异常: %s", actualMsg.GetID())
				}
			},
		},
		// 场景3：消息未设置Prefix，验证使用默认前缀
		{
			name: "正常追加-使用默认Prefix",
			msg: &Message{
				ID:         "test_msg_003",
				Stream:     "test_03",
				Values:     map[string]interface{}{"key3": "value3"},
				ErrorCount: 0,
			},
			wantErr: false,
			checkFunc: func(t *testing.T, msg storage.Messager) {
				// 队列Key应为默认前缀: test_prefix:test_stream
				queueKey := fmt.Sprintf("%s:%s", testQueue.defaultPrefix, msg.GetStream())
				msgList, err := testRdb.LRange(ctx, queueKey, 0, -1).Result()
				if err != nil || len(msgList) == 0 {
					t.Fatalf("读取队列失败或无消息: %v", err)
				}

				var actualMsg Message
				_ = json.Unmarshal([]byte(msgList[0]), &actualMsg)
				if actualMsg.GetPrefix() != "" {
					t.Errorf("消息未设置Prefix时，Prefix应为空（队列Key使用默认），实际: %s", actualMsg.GetPrefix())
				}
			},
		},
		// 场景4：队列关闭后追加消息，验证返回错误
		{
			name: "队列关闭后追加-返回错误",
			msg: &Message{
				ID:     "test_msg_004",
				Stream: "test_stream",
				Values: map[string]interface{}{"key4": "value4"},
			},
			wantErr: true,
			prepareFunc: func() {
				// 先关闭队列
				testQueue.Shutdown()
				// 等待关闭生效
				time.Sleep(100 * time.Millisecond)
			},
			checkFunc: func(t *testing.T, msg storage.Messager) {
				// 校验队列中无该消息
				queueKey := fmt.Sprintf("%s:%s", msg.GetPrefix(), msg.GetStream())
				count, err := testRdb.LLen(ctx, queueKey).Result()
				if err != nil {
					t.Fatalf("读取队列长度失败: %v", err)
				}
				if count > 0 {
					t.Error("队列关闭后追加消息，队列中不应有该消息")
				}
			},
		},
		// 场景5：消息序列化失败（Values包含不可序列化类型）
		{
			name: "消息序列化失败-返回错误",
			msg: &Message{
				ID:     "test_msg_005",
				Stream: "test_stream",
				// 构造不可序列化的Values（循环引用）
				Values: func() map[string]interface{} {
					m := make(map[string]interface{})
					m["self"] = m // 循环引用，JSON序列化失败
					return m
				}(),
			},
			wantErr: true,
			checkFunc: func(t *testing.T, msg storage.Messager) {
				// 校验队列中无该消息
				queueKey := fmt.Sprintf("%s:%s", msg.GetPrefix(), msg.GetStream())
				count, err := testRdb.LLen(ctx, queueKey).Result()
				if err != nil {
					t.Fatalf("读取队列长度失败: %v", err)
				}
				if count > 0 {
					t.Error("序列化失败的消息不应被追加到队列")
				}
			},
		},
	}

	// 执行所有测试用例
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// 执行测试前准备逻辑
			if tt.prepareFunc != nil {
				tt.prepareFunc()
			}

			// 执行Append方法
			err := testQueue.Append(tt.msg)

			// 校验错误是否符合预期
			if (err != nil) != tt.wantErr {
				t.Errorf("Append() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// 执行自定义校验逻辑
			if tt.checkFunc != nil {
				tt.checkFunc(t, tt.msg)
			}

			// 清理当前用例的队列数据（保证下一个用例独立）
			queueKey := fmt.Sprintf("%s:%s", tt.msg.GetPrefix(), tt.msg.GetStream())
			_ = testRdb.Del(ctx, queueKey).Err()
		})
	}
}

// 性能测试：测试Append方法的并发写入性能
func BenchmarkAppend(b *testing.B) {
	ctx := context.Background()
	// 初始化队列
	benchQueue := NewRedisQueue(testRdb, "bench_prefix", 3)
	defer benchQueue.Shutdown()

	// 构造测试消息
	testMsg := &Message{
		Stream: "bench_stream",
		Values: map[string]interface{}{
			"bench_key": "bench_value",
			"num":       100,
		},
	}

	// 重置性能测试计时器
	b.ResetTimer()

	// 并发写入
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = benchQueue.Append(testMsg)
		}
	})

	// 校验写入总数
	queueKey := fmt.Sprintf("%s:%s", benchQueue.defaultPrefix, testMsg.GetStream())
	count, err := testRdb.LLen(ctx, queueKey).Result()
	if err != nil {
		b.Fatalf("读取队列长度失败: %v", err)
	}
	if int(count) != b.N {
		b.Errorf("并发写入总数不匹配，期望: %d, 实际: %d", b.N, count)
	}

	// 清理数据
	_ = testRdb.Del(ctx, queueKey).Err()
}

// 测试Register方法的核心场景
func TestRedisQueue_Register(t *testing.T) {
	// 初始化测试用队列实例（使用测试专用Redis DB）
	testQueue := NewRedisQueue(testRdb, "test_prefix", 3)
	defer testQueue.Shutdown() // 测试后关闭队列

	// 定义测试用的合法消费函数
	validConsumer := func(msg storage.Messager) error {
		return nil
	}

	// 定义测试用例
	tests := []struct {
		name         string                                         // 测试用例名称
		consumerName string                                         // 待注册的消费者名称
		consumerFunc storage.ConsumerFunc                           // 待注册的消费函数
		expectPanic  bool                                           // 是否期望触发panic
		checkFunc    func(t *testing.T, q *RedisQueue, name string) // 自定义校验逻辑
	}{
		// 场景1：正常注册（有效名称+有效函数）
		{
			name:         "正常注册-有效名称和函数",
			consumerName: "test_consumer_001",
			consumerFunc: validConsumer,
			expectPanic:  false,
			checkFunc: func(t *testing.T, q *RedisQueue, name string) {
				// 从sync.Map中读取已注册的函数，验证是否匹配
				val, ok := q.consumers.Load(name)
				if !ok {
					t.Fatalf("消费者 %s 未成功注册，sync.Map中无该键", name)
				}

				// 类型断言为storage.ConsumerFunc
				registeredFunc, ok := val.(storage.ConsumerFunc)
				if !ok {
					t.Fatalf("消费者 %s 注册的函数类型错误，非storage.ConsumerFunc", name)
				}

				// 验证函数地址一致（确保存储的是同一个函数）
				if registeredFunc == nil {
					t.Fatalf("消费者 %s 注册的函数为nil", name)
				}
			},
		},
		// 场景2：注册空名称的消费者（期望触发panic）
		{
			name:         "注册空名称-触发panic",
			consumerName: "", // 空名称
			consumerFunc: validConsumer,
			expectPanic:  true,
			checkFunc:    nil, // panic场景无需后续校验
		},
		// 场景3：注册nil消费函数（期望触发panic）
		{
			name:         "注册nil函数-触发panic",
			consumerName: "test_consumer_003",
			consumerFunc: nil, // nil函数
			expectPanic:  true,
			checkFunc:    nil,
		},
		// 场景4：重复注册同一名称（验证覆盖原有函数）
		{
			name:         "重复注册-覆盖原有函数",
			consumerName: "test_consumer_004",
			consumerFunc: validConsumer,
			expectPanic:  false,
			checkFunc: func(t *testing.T, q *RedisQueue, name string) {
				// 第一步：验证首次注册成功
				_, ok := q.consumers.Load(name)
				if !ok {
					t.Fatalf("首次注册消费者 %s 失败", name)
				}

				// 第二步：定义新的消费函数，重复注册同一名称
				newConsumer := func(msg storage.Messager) error {
					return errors.New("new func error")
				}
				q.Register(name, newConsumer)

				// 第三步：验证sync.Map中存储的是新函数
				val2, ok := q.consumers.Load(name)
				if !ok {
					t.Fatalf("重复注册后消费者 %s 丢失", name)
				}

				// 验证新旧函数
				newFunc := val2.(storage.ConsumerFunc)
				testMsg := &Message{ID: "test_msg_004"}
				if err := newFunc(testMsg); err == nil || err.Error() != "new func error" {
					// 与预期新覆盖的函数不一致
					t.Error("重复注册后消费函数未被覆盖，仍为原函数")
				}
			},
		},
		// 场景5：并发注册多个消费者（验证sync.Map并发安全）
		{
			name:         "并发注册-多个消费者",
			consumerName: "test_consumer_005",
			consumerFunc: validConsumer,
			expectPanic:  false,
			checkFunc: func(t *testing.T, q *RedisQueue, name string) {
				var wg sync.WaitGroup
				consumerCount := 100 // 并发注册100个消费者
				names := make([]string, consumerCount)

				// 并发注册不同名称的消费者
				for i := 0; i < consumerCount; i++ {
					wg.Add(1)
					names[i] = name + "_" + string(rune(i))
					go func(idx int) {
						defer wg.Done()
						q.Register(names[idx], validConsumer)
					}(i)
				}
				wg.Wait()

				// 验证所有消费者都注册成功
				for _, n := range names {
					if _, ok := q.consumers.Load(n); !ok {
						t.Errorf("并发注册失败，消费者 %s 未找到", n)
					}
				}
			},
		},
	}

	// 执行所有测试用例
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 捕获panic（针对期望panic的场景）
			defer func() {
				if r := recover(); r != nil {
					if !tt.expectPanic {
						t.Errorf("Register() 意外触发panic: %v", r)
					} else {
						t.Logf("Register() 按预期触发panic: %v", r)
					}
				} else if tt.expectPanic {
					t.Error("Register() 未触发预期的panic")
				}
			}()

			// 执行Register方法
			testQueue.Register(tt.consumerName, tt.consumerFunc)

			// 执行自定义校验逻辑（非panic场景）
			if !tt.expectPanic && tt.checkFunc != nil {
				tt.checkFunc(t, testQueue, tt.consumerName)
			}

			// 清理当前用例的注册数据（保证下一个用例独立）
			if tt.consumerName != "" {
				testQueue.consumers.Delete(tt.consumerName)
			}
		})
	}
}
