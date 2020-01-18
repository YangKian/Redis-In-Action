package redisConn

import (
	"github.com/go-redis/redis"
	"log"
	"redisInAction/pkg/common"
)

type RedisClient redis.Client

func ConnectRedis() *RedisClient {
	conn := redis.NewClient(&redis.Options{
		Addr:     common.Addr,
		Password: common.Password,
		DB:       common.DB,
	})

	if _, err := conn.Ping().Result(); err != nil {
		log.Fatalf("Connect to redis client failed, err: %v\n", err)
	}
	return (*RedisClient)(conn)
}

