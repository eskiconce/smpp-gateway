package queue

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	rdb *redis.Client
}

func NewRedis(url string) (*RedisQueue, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}
	return &RedisQueue{rdb: rdb}, nil
}

func (q *RedisQueue) Close() error { return q.rdb.Close() }

func (q *RedisQueue) Enqueue(ctx context.Context, key string, it Item) error {
	b, err := json.Marshal(it)
	if err != nil {
		return err
	}
	return q.rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]any{"item": string(b)}}).Err()
}

func (q *RedisQueue) Consume(ctx context.Context, key, group string, fn func(Item) error) error {
	if _, err := q.rdb.XGroupCreateMkStream(ctx, key, group, "0").Result(); err != nil {
		if !isBusyGroup(err) {
			return err
		}
	}
	for {
		res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: group, Streams: []string{key, ">"}, Count: 1, Block: 0,
		}).Result()
		if err != nil {
			return err
		}
		for _, stream := range res {
			for _, msgID := range stream.Messages {
				raw, ok := msgID.Values["item"].(string)
				if !ok {
					q.rdb.XAck(ctx, key, group, msgID.ID)
					continue
				}
				var it Item
				if err := json.Unmarshal([]byte(raw), &it); err != nil {
					q.rdb.XAck(ctx, key, group, msgID.ID)
					continue
				}
				if err := fn(it); err != nil {
					return err
				}
				if err := q.Ack(ctx, key, group, msgID.ID); err != nil {
					return err
				}
			}
		}
	}
}

func (q *RedisQueue) Ack(ctx context.Context, key, group, streamID string) error {
	return q.rdb.XAck(ctx, key, group, streamID).Err()
}

func isBusyGroup(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}
