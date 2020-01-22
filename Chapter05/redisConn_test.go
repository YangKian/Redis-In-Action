package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"redisInAction/Chapter05/common"
	"redisInAction/Chapter05/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"testing"
	"time"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	t.Run("Test log recent", func(t *testing.T) {
		t.Log("Let's write a few logs to the recent log")
		for i := 0; i < 5; i++ {
			client.LogRecent("test", fmt.Sprintf("this is message %d", i), "info", nil)
		}
		recent := client.Conn.LRange("recent:test:info", 0, -1).Val()
		t.Log("The current recent message log has this many messages:", len(recent))
		t.Log("Those messages include:")
		for _, v := range recent {
			t.Log(v)
		}
		utils.AssertTrue(t, len(recent) >= 5)
		defer client.Conn.FlushAll()
	})

	t.Run("Test log common", func(t *testing.T) {
		t.Log("Let's write some items to the common log")
		for i := 1; i < 6; i++ {
			for j := 0; j < i; j++ {
				client.LogCommon("test", fmt.Sprintf("message-%d", i), "info", 5)
			}
		}
		commons := client.Conn.ZRevRangeWithScores("common:test:info", 0, -1).Val()
		t.Log("The current number of common messages is:", len(commons))
		t.Log("Those common messages are:")
		for _, v := range commons {
			t.Log(v)
		}
		utils.AssertTrue(t, len(commons) >= 5)
		defer client.Conn.FlushAll()
	})

	t.Run("Test counters", func(t *testing.T) {
		t.Log("Let's update some counters for now and a little in the future")
		now := time.Now().Unix()
		for i := 0; i < 10; i++ {
			client.UpdateCounter("test", int64(rand.Intn(5)+1), now+int64(i))
		}
		counter := client.GetCount("test", "1")
		t.Log("We have some per-second counters:", len(counter))
		utils.AssertTrue(t, len(counter) >= 10)
		counter = client.GetCount("test", "5")
		t.Log("We have some per-5-second counters:", len(counter))
		t.Log("These counters include:")
		count := 0
		for _, v := range counter {
			if count == 10 {
				break
			}
			t.Log(v)
			count++
		}
		utils.AssertTrue(t, len(counter) >= 2)

		t.Log("Let's clean out some counters by setting our sample count to 0")
		common.SAMPLECOUNT = 0
		go client.CleanCounters()
		time.Sleep(1 * time.Second)
		common.QUIT = true
		counter = client.GetCount("test", "86400")
		t.Log("Did we clean out all of the counters?", len(counter))
		utils.AssertTrue(t, len(counter) == 0)
		defer client.Conn.FlushAll()
	})

	t.Run("Test stats", func(t *testing.T) {
		t.Log("Let's add some data for our statistics!")
		var res []redis.Cmder
		for i := 0; i < 5; i++ {
			res = client.UpdateStats("temp", "example", utils.RandomFloat(5, 15), 5)
		}
		t.Log("We have some aggregate statistics:", res)
		rr := client.GetStats("temp", "example")
		t.Log("Which we can also fetch manually:")
		for k, v := range rr {
			t.Log(k, v)
		}
		utils.AssertTrue(t, rr["count"] >= 5)
		defer client.Conn.FlushAll()
	})
}
