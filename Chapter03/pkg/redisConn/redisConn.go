package redisConn

import (
	"github.com/go-redis/redis"
	"log"
	"redisInAction/Chapter02/pkg/common"
	"redisInAction/config"
	"time"
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

func (r *RedisClient) Reset() {
	delKeys := []string{"key", "new-string-key", "another-key", "list-key", "list", "list2", "set-key", "set-key2",
		"skey1", "skey2", "hash-key", "hash-key2", "zset-key", "zset-1", "zset-2", "zset-i", "zset-u", "zset-u2", "set-1"}
	var toDel []string
	for _, v := range delKeys {
		toDel = append(toDel, r.Keys(v).Val()...)
	}

	if len(toDel) != 0 {
		r.Del(toDel...)
	}
	common.QUIT = false
	common.LIMIT = 10000000
	common.FLAG = 1
}

//=========================== the answer of exercise ==================================
func (r *RedisClient) UpdateToken(token, user, item string) {
	timestamp := time.Now().Unix()
	r.HSet("login:", token, user)
	r.ZAdd("recent", redis.Z{Score:float64(timestamp), Member:token})
	if item != "" {
		key := "viewed" + token
		r.LRem(key, 1, item)
		r.RPush(key, item)
		r.LTrim(key, -25, -1)
		r.ZIncrBy("viewed:", -1, item)
	}
}