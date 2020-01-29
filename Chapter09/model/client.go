package model

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"hash/crc32"
	"strconv"
	"time"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}

func (c *Client) LongZiplistPerformance(key string, length, passes, psize int) int64 {
	c.Conn.Del(key)
	pipeline := c.Conn.Pipeline()
	for i := 0; i < length; i++ {
		pipeline.RPush(key, i)
	}
	_, _ = pipeline.Exec()

	for p := 0; p < passes; p++ {
		for pi := 0; pi < psize; pi++ {
			pipeline.RPopLPush(key, key)
		}
		_, _ = pipeline.Exec()
	}
	return int64(passes*psize) / time.Now().Unix()
}

func (c *Client) ShardKey(base string, key string, totalElements, shardSize int) string {
	var shardId int64
	if k, err := strconv.ParseInt(key, 10, 64); err != nil {
		shardId = k / 10
	} else {
		shards := 2 * totalElements / shardSize
		shardId = int64(crc32.ChecksumIEEE([]byte(key))) % int64(shards)
	}
	return fmt.Sprintf("%s:%s", base, strconv.Itoa(int(shardId)))
}

func (c *Client) ShardHset(base, key string, value interface{}, totalElements, shardSize int) bool {
	shard := c.ShardKey(base, key, totalElements, shardSize)
	return c.Conn.HSet(shard, key, value).Val()
}

func (c *Client) ShardHget(base, key string, totalElements, shardSize int) string {
	shard := c.ShardKey(base, key, totalElements, shardSize)
	return c.Conn.HGet(shard, key).Val()
}
