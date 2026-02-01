package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xxx-newbee/storage"
)

type RedisQueue struct {
	rdb           *redis.Client
	defaultPrefix string
	ctx           context.Context
	cancel        context.CancelFunc
	consumers     sync.Map
	running       bool // 消费协程状态
	mtx           sync.RWMutex
	wg            sync.WaitGroup
	maxRetry      int
}

// 拼接队列完整Key: Prefix:Stream
func (r *RedisQueue) getQueueKey(msg storage.Messager) string {
	prefix := msg.GetPrefix()
	if prefix == "" {
		prefix = r.defaultPrefix
	}
	return fmt.Sprintf("%s:%s", prefix, msg.GetStream())
}

// 待确认队列Key: Prefix:Stream:pending
func (r *RedisQueue) getPendingKey(msg storage.Messager) string {
	return fmt.Sprintf("%s:pending", r.getQueueKey(msg))
}

// 死信队列Key: Prefix:Stream:dead
func (r *RedisQueue) getDeadKey(msg storage.Messager) string {
	return fmt.Sprintf("%s:dead", r.getQueueKey(msg))
}

func NewRedisQueue(rdb *redis.Client, prefix string, maxRetry int) *RedisQueue {
	ctx, cancel := context.WithCancel(context.Background())
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}
	return &RedisQueue{
		rdb:           rdb,
		defaultPrefix: prefix,
		ctx:           ctx,
		cancel:        cancel,
		maxRetry:      maxRetry,
		consumers:     sync.Map{},
		running:       false,
	}
}

func (r *RedisQueue) String() string {
	return "redis"
}

// Append 消息入队
func (r *RedisQueue) Append(msg storage.Messager) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if r.ctx.Err() != nil {
		return errors.New("queue is closed")
	}

	if msg.GetID() == "" {
		msg.SetID(fmt.Sprintf("msg_%d_%d", time.Now().Unix(), time.Now().UnixNano()%1000000))
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return errors.New("json marshal msg error")
	}
	queueKey := r.getQueueKey(msg)
	if err := r.rdb.LPush(r.ctx, queueKey, string(msgBytes)).Err(); err != nil {
		return errors.New("queue push message error")
	}
	return nil
}

// Register 注册消费函数
func (r *RedisQueue) Register(name string, f storage.ConsumerFunc) {
	if name == "" || f == nil {
		panic("queue register error")
	}
	r.consumers.Store(name, f)
}

// Run 启动消费者
func (r *RedisQueue) Run() {
	r.mtx.Lock()
	if r.running {
		r.mtx.Unlock()
		return
	}
	r.running = true
	r.mtx.Unlock()

	r.consumers.Range(func(k, v interface{}) bool {
		cNmae := k.(string)
		cFunc := v.(storage.ConsumerFunc)
		r.wg.Add(1)
		go r.ConsumerLoop(cNmae, cFunc)
		return true
	})
}

// ConsumerLoop 消费循环
func (r *RedisQueue) ConsumerLoop(name string, f storage.ConsumerFunc) {
	defer r.wg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return

		default:
			// 1. 获取所有消息
			queueKey := fmt.Sprintf("%s:*", r.defaultPrefix)
			keys, err := r.rdb.Keys(r.ctx, queueKey).Result()
			if err != nil || len(keys) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			// 2.堵塞读取消息队列，防止消息被多个消费者竞争
			result, err := r.rdb.BRPop(r.ctx, 5*time.Second, keys...).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				fmt.Printf("queue consumer [%s] err: %v\n", name, err)
				continue
			}
			queueKey = result[0]
			msgStr := result[1]

			// 3.反序列化
			var redisMsg Message
			if err := json.Unmarshal([]byte(msgStr), &redisMsg); err != nil {
				fmt.Printf("queue consumer [%s] unmarshal msg err: %v\n", name, err)
				continue
			}

			// 4.消息移动到待确认队列（原子事务）
			if err := r.moveToPending(&redisMsg, msgStr); err != nil {
				fmt.Printf("queue consumer [%s] move msg ID:[%s] to pending queue err: %v\n", name, redisMsg.GetID(), err)
			}

			// 5.执行消费函数
			err = f(&redisMsg)

			// 6.处理结果
			if err == nil {
				// 处理成功
				r.rdb.LRem(r.ctx, r.getPendingKey(&redisMsg), 1, msgStr)
				fmt.Printf("queue consumer [%s] consume msg success: ID=[%s] Stream=%s\n", name, redisMsg.GetID(), redisMsg.GetStream())
			} else {
				// 处理失败
				redisMsg.SetErrorCount(redisMsg.GetErrorCount() + 1)
				if redisMsg.GetErrorCount() >= r.maxRetry {
					// 超过最大重试次数，转入死信队列
					if err := r.moveToDeadLetter(&redisMsg, msgStr); err != nil {
						fmt.Printf("queue consumer [%s] move msg ID:[%s] to deadletter err: %v\n", name, redisMsg.GetID(), err)
					} else {
						fmt.Printf("queue consumer [%s] move msg ID:[%s] to deadletter, error count:%d\n", name, redisMsg.GetID(), redisMsg.GetErrorCount())
					}
				} else {
					updateMsgBytes, _ := json.Marshal(&redisMsg)
					updateMsgStr := string(updateMsgBytes)
					if err := r.retryMessage(&redisMsg, updateMsgStr, msgStr); err != nil {
						fmt.Printf("queue consumer [%s] consume msg ID:[%s] err: %v\n", name, redisMsg.GetID(), err)
					} else {
						fmt.Printf("queue consumer [%s] consume msg: ID=[%s] ErrorCount=[%d]\n", name, redisMsg.GetID(), redisMsg.GetErrorCount())
					}

				}
				// 删除原待确认队列中的旧消息
				_ = r.rdb.LRem(r.ctx, r.getPendingKey(&redisMsg), 1, msgStr).Err()
			}
		}
	}
}

// 消息移动到待确认队列，弱原子操作
func (r *RedisQueue) moveToPending(msg storage.Messager, msgStr string) error {
	_, err := r.rdb.TxPipelined(r.ctx, func(p redis.Pipeliner) error {
		p.LPush(r.ctx, r.getPendingKey(msg), msgStr)
		p.LRem(r.ctx, r.getQueueKey(msg), 1, msgStr)
		return nil
	})
	return err
}

// 消息移动到死信队列
func (r *RedisQueue) moveToDeadLetter(msg storage.Messager, msgStr string) error {
	_, err := r.rdb.TxPipelined(r.ctx, func(p redis.Pipeliner) error {
		p.LPush(r.ctx, r.getDeadKey(msg), msgStr)
		p.LRem(r.ctx, r.getPendingKey(msg), 1, msgStr)
		return nil
	})
	return err
}

// 重试，消息从pending队列移到未处理队列
func (r *RedisQueue) retryMessage(msg storage.Messager, newMsg, oldMsg string) error {
	_, err := r.rdb.Pipelined(r.ctx, func(p redis.Pipeliner) error {
		p.LPush(r.ctx, r.getQueueKey(msg), newMsg)
		p.LRem(r.ctx, r.getPendingKey(msg), 1, oldMsg)
		return nil
	})
	return err
}

func (r *RedisQueue) Shutdown() {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if r.ctx.Err() != nil {
		return
	}
	r.cancel()

	if r.running {
		r.wg.Wait()
		r.running = false
	}

	if err := r.rdb.Close(); err != nil {
		fmt.Println(err)
	}
	fmt.Println("queue shutdown smoothly!")

}
