package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"redisInAction/Chapter03/pkg/redisConn"
	"sync/atomic"
	"time"
)

func Publisher(n int, conn *redisConn.RedisClient) {
	time.Sleep(1 * time.Second)
	for n > 0 {
		conn.Publish("channel", n)
		n--
	}
}

func RunPubsub(conn *redisConn.RedisClient, stop chan struct{}) {
	pubsub := (*redis.Client)(conn).Subscribe("channel")
	var count int32 = 0
	for item := range pubsub.Channel() {
		fmt.Println(item)
		atomic.AddInt32(&count, 1)
		fmt.Println(count)
		switch count {
		case 4:
			if err := pubsub.Unsubscribe("channel"); err != nil {
				log.Println("unsubscribe faile, err: ", err)
			} else {
				fmt.Println("unsubscribe success")
			}
		case 5:
			break
		default:
		}
	}
	stop <- struct{}{}
}

//TODO：使用通道后结果不对，会阻塞在main函数中接收通道处，使用有缓冲通道和无缓冲通道结果也不同
func main() {
	conn := redisConn.ConnectRedis()
	stop := make(chan struct{}, 1)
	go RunPubsub(conn, stop)
	Publisher(6, conn)
	<-stop
	time.Sleep(5 * time.Second)
}
