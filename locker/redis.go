package locker

import (
	"context"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

type RedisLocker struct {
	client *redis.Client
	mutex  *redislock.Client
}

func (r *RedisLocker) String() string {
	return "redis"
}

func (r *RedisLocker) Lock(key string, ttl int64, options *redislock.Options) (*redislock.Lock, error) {
	if r.mutex == nil {
		r.mutex = redislock.New(r.client)
	}
	return r.mutex.Obtain(context.TODO(), key, time.Duration(ttl)*time.Second, options)
}
