package redisConn

import (
	"github.com/go-redis/redis"
	"log"
	"redisInAction/config"
)

type RedisClient redis.Client

func ConnectRedis() *RedisClient {
	conn := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})

	if _, err := conn.Ping().Result(); err != nil {
		log.Fatalf("Connect to redis client failed, err: %v\n", err)
	}
	return (*RedisClient)(conn)
}

func (r *RedisClient) ProcessLogs(path string, callback func()) {
	currentFile := r.MGet("progress:file").Val()
	offset := r.MGet("progress:position").Val()

}

func (r *RedisClient) Reset() {
	delKeys := []string{"key", "new-string-key", "another-key", "list-key", "list", "list2", "set-key", "set-key2",
		"skey1", "skey2", "hash-key", "hash-key2", "zset-key", "zset-1", "zset-2", "zset-i", "zset-u", "zset-u2",
		"set-1", "sort-input", "d-*", "notrans: ", "trans:", "key"}
	var toDel []string
	for _, v := range delKeys {
		toDel = append(toDel, r.Keys(v).Val()...)
	}

	if len(toDel) != 0 {
		r.Del(toDel...)
	}
}
