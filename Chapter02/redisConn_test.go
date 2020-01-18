package main

import (
	"redisInAction/Chapter01/pkg/redisConn"
	"redisInAction/config"
)

func init() {
	config.DB = 0
	config.Addr = "192.168.1.6:6379"
	config.Password = ""
}

func main() {
	conn := redisConn.ConnectRedis()



	delKeys := []string{"time:*", "voted:*", "score:*", "article:*", "group:*"}
	var toDel []string
	for _, v := range delKeys {
		toDel = append(toDel, conn.Keys(v).Val()...)
	}

	if len(toDel) != 0 {
		conn.Del(toDel...)
	}
}
