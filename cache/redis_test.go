package cache

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var testRdb = redis.NewClient(&redis.Options{
	Addr:     "0.0.0.0:6379",
	Password: "",
	DB:       15, // 测试专用DB
})

func TestNewRedis(t *testing.T) {
	// 场景1：传入 nil 客户端，自动创建新客户端
	redisIns, err := NewRedis(nil, redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, redisIns)
	assert.NotNil(t, redisIns.client)

	// 场景2：传入已有客户端，复用客户端
	redisIns2, err := NewRedis(testRdb, redis.Options{})
	assert.NoError(t, err)
	assert.Equal(t, testRdb, redisIns2.client)

	// 场景3：Redis 连接失败（故意写错地址）
	_, err = NewRedis(nil, redis.Options{
		Addr: "localhost:6380", // 错误端口
	})
	assert.Error(t, err)
}

func TestRedis_SetAndGet(t *testing.T) {
	redisIns, err := NewRedis(testRdb, redis.Options{})
	assert.NoError(t, err)
	defer redisIns.client.FlushDB(context.TODO()) // 测试后清空 DB

	// 测试普通值设置与获取
	key := "test_key"
	val := "test_value"
	expire := 10 // 过期时间 10 秒

	// 执行 Set
	err = redisIns.Set(key, val, expire)
	assert.NoError(t, err)

	// 执行 Get
	getVal, err := redisIns.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, val, getVal)

	// 测试不存在的 key
	nonExistVal, err := redisIns.Get("non_exist_key")
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err) // Redis 未找到 key 的错误类型
	assert.Empty(t, nonExistVal)
}

// 测试 Del 方法
func TestRedis_Del(t *testing.T) {
	redisIns, err := NewRedis(testRdb, redis.Options{})
	assert.NoError(t, err)
	defer redisIns.client.FlushDB(context.TODO())

	key := "del_test_key"
	// 先设置值
	err = redisIns.Set(key, "del_value", 10)
	assert.NoError(t, err)

	// 删除 key
	err = redisIns.Del(key)
	assert.NoError(t, err)

	// 验证 key 已被删除
	_, err = redisIns.Get(key)
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
}

// 测试 HashGet + HashDel 方法
func TestRedis_HashGetAndHashDel(t *testing.T) {
	redisIns, err := NewRedis(testRdb, redis.Options{})
	assert.NoError(t, err)
	defer redisIns.client.FlushDB(context.TODO())

	hk := "test_hash" // hash 键
	key := "hash_key"
	val := "hash_value"

	// 先通过原生客户端设置 hash 值（模拟业务场景）
	err = redisIns.client.HSet(context.TODO(), hk, key, val).Err()
	assert.NoError(t, err)

	// 测试 HashGet
	getVal, err := redisIns.HashGet(hk, key)
	assert.NoError(t, err)
	assert.Equal(t, val, getVal)

	// 测试 HashDel
	err = redisIns.HashDel(hk, key)
	assert.NoError(t, err)

	// 验证 hash 字段已被删除
	_, err = redisIns.HashGet(hk, key)
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
}

// 测试 Increase + Decrease 方法
func TestRedis_IncreaseAndDecrease(t *testing.T) {
	redisIns, err := NewRedis(testRdb, redis.Options{})
	assert.NoError(t, err)
	defer redisIns.client.FlushDB(context.TODO())

	key := "counter_key"
	// 初始化值为 0
	err = redisIns.Set(key, 0, 10)
	assert.NoError(t, err)

	// 自增 1
	err = redisIns.Increase(key)
	assert.NoError(t, err)
	val, _ := redisIns.Get(key)
	assert.Equal(t, "1", val)

	// 自减 1
	err = redisIns.Decrease(key)
	assert.NoError(t, err)
	val, _ = redisIns.Get(key)
	assert.Equal(t, "0", val)
}

// 测试 Expire 方法
func TestRedis_Expire(t *testing.T) {
	redisIns, err := NewRedis(testRdb, redis.Options{})
	assert.NoError(t, err)
	defer redisIns.client.FlushDB(context.TODO())

	key := "expire_key"
	// 先设置值（无过期时间）
	err = redisIns.Set(key, "expire_value", 0) // expire=0 表示永不过期
	assert.NoError(t, err)

	// 设置过期时间 2 秒
	err = redisIns.Expire(key, 2*time.Second)
	assert.NoError(t, err)

	// 验证过期时间
	ttl := redisIns.client.TTL(context.TODO(), key).Val()
	assert.True(t, ttl <= 2*time.Second && ttl > 0)

	// 等待 3 秒，验证 key 过期
	time.Sleep(3 * time.Second)
	_, err = redisIns.Get(key)
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
}

// 测试 String 方法
func TestRedis_String(t *testing.T) {
	redisIns, err := NewRedis(testRdb, redis.Options{})
	assert.NoError(t, err)
	assert.Equal(t, "redis", redisIns.String())
}
