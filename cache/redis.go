package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xxx-newbee/storage"
)

type Redis struct {
	client *redis.Client
}

func NewRedis(client *redis.Client, opts redis.Options) (storage.AdapterCache, error) {
	if client == nil {
		client = redis.NewClient(&opts)
	}
	r := &Redis{client: client}
	if err := r.connect(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Redis) connect() error {
	_, err := r.client.Ping(context.Background()).Result()
	return err
}

func (r *Redis) String() string {
	return "redis"
}

func (r *Redis) Get(key string) (string, error) {
	return r.client.Get(context.TODO(), key).Result()
}

func (r *Redis) Set(key string, val interface{}, expire int) error {
	return r.client.Set(context.TODO(), key, val, time.Duration(expire)*time.Second).Err()
}

func (r *Redis) Del(key string) error {
	return r.client.Del(context.TODO(), key).Err()
}

func (r *Redis) HashGet(hk, key string) (string, error) {
	return r.client.HGet(context.TODO(), hk, key).Result()
}

func (r *Redis) HashDel(hk, key string) error {
	return r.client.HDel(context.TODO(), hk, key).Err()
}

func (r *Redis) Increase(key string) error {
	return r.client.Incr(context.TODO(), key).Err()
}

func (r *Redis) Decrease(key string) error {
	return r.client.Decr(context.TODO(), key).Err()
}

func (r *Redis) Expire(key string, dur time.Duration) error {
	return r.client.Expire(context.TODO(), key, dur).Err()
}
