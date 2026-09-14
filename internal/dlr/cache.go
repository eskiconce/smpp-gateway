package dlr

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache interface {
	Register(ctx context.Context, key, value string) error
	Lookup(ctx context.Context, key string) (string, error)
	Delete(ctx context.Context, key string) error
}

type MemCache struct {
	mu    sync.Mutex
	items map[string]string
}

func NewMemCache() *MemCache { return &MemCache{items: map[string]string{}} }

func (c *MemCache) Register(_ context.Context, key, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = value
	return nil
}

func (c *MemCache) Lookup(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.items[key], nil
}

func (c *MemCache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
	return nil
}

const keyPrefix = "dlr:pend:"

type RedisCache struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewRedisCache(url string, ttl time.Duration) (*RedisCache, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}
	return &RedisCache{rdb: rdb, ttl: ttl}, nil
}

func (c *RedisCache) Close() error { return c.rdb.Close() }

func (c *RedisCache) Register(ctx context.Context, key, value string) error {
	return c.rdb.Set(ctx, keyPrefix+key, value, c.ttl).Err()
}

func (c *RedisCache) Lookup(ctx context.Context, key string) (string, error) {
	v, err := c.rdb.Get(ctx, keyPrefix+key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

func (c *RedisCache) Delete(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, keyPrefix+key).Err()
}
