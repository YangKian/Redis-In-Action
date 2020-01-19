package redisConn

import (
	"encoding/json"
	"github.com/go-redis/redis"
	"log"
	"redisInAction/Chapter02/pkg/common"
	"redisInAction/Chapter02/repository"
	"redisInAction/config"
	"redisInAction/utils"
	"sync/atomic"
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

func (r *RedisClient) CheckToken(token string) string {
	return r.HGet("login:", token).Val()
}

func (r *RedisClient) UpdateToken(token, user, item string) {
	timestamp := time.Now().Unix()
	r.HSet("login:", token, user)
	r.ZAdd("recent", redis.Z{Score:float64(timestamp), Member:token})
	if item != "" {
		r.HSet("viewed:" + token, item, timestamp)
		r.ZRemRangeByRank("viewed:" + token, 0, -26)
	}
}

func (r *RedisClient) CleanSessions() {
	for !common.QUIT {
		size := r.ZCard("recent:").Val()
		if size <= common.LIMIT {
			time.Sleep(1 * time.Second)
			continue
		}

		endIndex := utils.Min(size-common.LIMIT, 100)
		tokens := r.ZRange("recent:", 0, endIndex-1).Val()

		var sessionKey []string
		for _, token := range tokens {
			sessionKey = append(sessionKey, token)
		}

		r.Del(sessionKey...)
		r.HDel("login:", tokens...)
		r.ZRem("recent:", tokens)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func (r *RedisClient) AddToCart(session, item string, count int) {
	switch {
	case count <= 0:
		r.HDel("cart:" + session, item)
	default:
		r.HSet("cart:" + session, item, count)
	}
}

func (r *RedisClient) CleanFullSession() {
	for !common.QUIT {
		size := r.ZCard("recent:").Val()
		if size <= common.LIMIT {
			time.Sleep(1 * time.Second)
			continue
		}

		endIndex := utils.Min(size-common.LIMIT, 100)
		sessions := r.ZRange("recent:", 0, endIndex - 1).Val()

		var sessionKeys []string
		for _, sess := range sessions {
			sessionKeys = append(sessionKeys, "viewed:" + sess)
			sessionKeys = append(sessionKeys, "cart:" + sess)
		}

		r.Del(sessionKeys...)
		r.HDel("login:", sessions...)
		r.ZRem("recent:", sessions)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

//func (r *RedisClient) CacheRequest(request string, callback func(string) string) string {
	//if ! CanCache() {
	//	return callback(request)
	//}
	//
	//pageKey := "cache:" + helper.HashRequest(request)
	//content := r.Get(pageKey).Val()
	//
	//if content == "" {
	//	content = callback(request)
	//	r.Set(pageKey, content, 300 * time.Second)
	//}
	//return content
//}

func (r *RedisClient) ScheduleRowCache(rowId string, delay int64) {
	r.ZAdd("delay:", redis.Z{Member:rowId, Score:float64(delay)})
	r.ZAdd("schedule:", redis.Z{Member:rowId, Score:float64(time.Now().Unix())})
}

func (r *RedisClient) CacheRows() {
	for ! common.QUIT {
		next := r.ZRangeWithScores("schedule:", 0, 0).Val()
		now := time.Now().Unix()
		if next == nil || next[0].Score > float64(now) {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		rowId := next[0].Member.(string)
		delay := r.ZScore("delay:", rowId).Val()
		if delay <= 0 {
			r.ZRem("delay:", rowId)
			r.ZRem("schedule:", rowId)
			r.Del("inv:" + rowId)
			continue
		}

		row := repository.Get(rowId) //TODO:明确到底怎么使用
		r.ZAdd("schedule:", redis.Z{Member:rowId, Score: float64(now) + delay})
		jsonRow, err := json.Marshal(row)
		if err == nil {
			log.Fatalf("marshel json failed, data is: %v, err is: %v\n", row, err)
		}
		r.Set("inv:" + rowId, jsonRow, 0)
	}
}

func (r *RedisClient) UpdateTokenModified(token, user string, item string) {
	timestamp := time.Now().Unix()
	r.HSet("login:", token, user)
	r.ZAdd("recent:", redis.Z{Score:float64(timestamp), Member:token})
	if item != "" {
		r.HSet("viewed:" + token, item, timestamp)
		r.ZRemRangeByRank("viewed:" + token, 0, -26)
		r.ZIncrBy("viewed:", -1, item)
	}
}

func (r *RedisClient) RescaleViewed() {
	for ! common.QUIT {
		r.ZRemRangeByRank("viewed:", 20000, -1)
		r.ZInterStore("viewed:", redis.ZStore{Weights:[]float64{0.5}}, "viewed:")
		time.Sleep(300 * time.Second)
	}
}

func (r *RedisClient) CanCache(request string) {
	//itemId :=
}

func (r *RedisClient) Reset() {
	delKeys := []string{"login:*", "recent:*", "viewed:*", "cart:*", "cache:*", "delay:*", "schedule:*", "inv:*"}
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
