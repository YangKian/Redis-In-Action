package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"redisInAction/Chapter03/pkg/redisConn"
	"time"
)

func NotRans(conn *redisConn.RedisClient) {
	fmt.Println(conn.Incr("notrans: ").Val())
	time.Sleep(100 * time.Millisecond)
	fmt.Println(conn.Decr("notrans: ").Val())
}

func Trans(conn *redisConn.RedisClient) {
	pipeline := (*redis.Client)(conn).Pipeline()
	pipeline.Incr("trans:")
	time.Sleep(100 * time.Millisecond)
	pipeline.Decr("trans:")
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline failed, the err is: ", err)
	}
	fmt.Println(res[0])
}

func main() {
	conn := redisConn.ConnectRedis()
	for i := 0; i < 3; i++ {
		go NotRans(conn)
	}
	time.Sleep(500 * time.Millisecond)
	fmt.Println("=======================================")

	for i := 0; i < 3; i++ {
		go Trans(conn)
	}
	time.Sleep(500 * time.Millisecond)
	defer conn.Reset()
}
