package main

import (
	"redisInAction/Chapter09/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"strings"
	"testing"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	t.Run("ziplist", func(t *testing.T) {
		client.Conn.RPush("test", "a", "b", "c", "d")
		t.Log(client.Conn.DebugObject("test").Val())
		client.Conn.RPush("test", "e", "f", "g", "h")
		t.Log(client.Conn.DebugObject("test").Val())
		client.Conn.RPush("test", strings.Repeat("a", 65))
		t.Log(client.Conn.DebugObject("test").Val())
		t.Log(client.Conn.LLen("test"))
		client.Conn.RPop("test")
		t.Log(client.Conn.DebugObject("test").Val())
		defer client.Conn.FlushAll()
	})

	t.Run("intset", func(t *testing.T) {
		pipeline := client.Conn.TxPipeline()

		for i := 0; i < 500; i++ {
			pipeline.SAdd("set-object", i)
		}
		_, _ = pipeline.Exec()
		t.Log(client.Conn.DebugObject("set-object").Val())
		for i := 500; i < 1000; i++ {
			pipeline.SAdd("set-object", i)
		}
		_, _ = pipeline.Exec()
		t.Log(client.Conn.DebugObject("set-object").Val())
		t.Log(client.Conn.ConfigGet("*").String())
	})

	t.Run("Test long ziplist performance", func(t *testing.T) {
		client.LongZiplistPerformance("test", 5, 10, 10)
		utils.AssertnumResult(t, 5, client.Conn.LLen("test").Val())
		defer client.Conn.FlushAll()
	})
}
