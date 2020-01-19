package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"redisInAction/Chapter03/pkg/redisConn"
	"testing"
	"time"
)

func TestLoginCookies(t *testing.T) {
	conn := redisConn.ConnectRedis()
	
	t.Run("Test INCR and DECR", func(t *testing.T) {
		conn.Get("key")
		res := conn.Incr("key").Val()
		assertnumResult(t, 1, res)
		res = conn.IncrBy("key", 15).Val()
		assertnumResult(t, 16, res)
		res = conn.DecrBy("key", 5).Val()
		assertnumResult(t, 11, res)
		succ := conn.Set("key", 13, 0).Val()
		assertStringResult(t, "OK", succ)
		res, _ = conn.Get("key").Int64()
		assertnumResult(t, 13, res)
		defer conn.Reset()
	})

	t.Run("Operation on substring and bit", func(t *testing.T) {
		res := conn.Append("new-string-key", "hello ").Val()
		assertnumResult(t, 6, res)
		res = conn.Append("new-string-key", "world!").Val()
		assertnumResult(t, 12, res)
		str := conn.GetRange("new-string-key", 3, 7).Val()
		assertStringResult(t, "lo wo", str)
		res = conn.SetRange("new-string-key", 0, "H").Val()
		conn.SetRange("new-string-key", 6, "W")
		assertnumResult(t, 12, 12)
		str = conn.Get("new-string-key").Val()
		assertStringResult(t, "Hello World!", str)
		res = conn.SetRange("new-string-key", 11, ", how are you?").Val()
		assertnumResult(t, 25, res)
		str = conn.Get("new-string-key").Val()
		assertStringResult(t, "Hello World, how are you?", str)
		res = conn.SetBit("another-key", 2, 1).Val()
		assertnumResult(t, 0, 0)
		res = conn.SetBit("another-key", 7, 1).Val()
		assertnumResult(t, 0, 0)
		str = conn.Get("another-key").Val()
		assertStringResult(t, "!", str)
		defer conn.Reset()
	})
	
	t.Run("Operation on list", func(t *testing.T) {
		conn.RPush("list-key", "last")
		conn.LPush("list-key", "first")
		res := conn.RPush("list-key", "new last").Val()
		assertnumResult(t, 3, res)
		lst := conn.LRange("list-key", 0, -1).Val()
		t.Log("the list is: ", lst)
		str := conn.LPop("list-key").Val()
		assertStringResult(t, "first", str)
		str = conn.LPop("list-key").Val()
		assertStringResult(t, "last", str)
		lst = conn.LRange("list-key", 0, -1).Val()
		t.Log("the list is: ", lst)
		res = conn.RPush("list-key", "a", "b", "c").Val()
		assertnumResult(t, 4, res)
		conn.LTrim("list-key", 2, -1)
		t.Log("the list is: ", fmt.Sprintf("%v", conn.LRange("list-key", 0, -1).Val()))
		defer conn.Reset()
	})

	t.Run("Block pop", func(t *testing.T) {
		conn.RPush("list", "item1")
		conn.RPush("list", "item2")
		conn.RPush("list2", "item3")
		item := conn.BRPopLPush("list2", "list", 1 * time.Second).Val()
		assertStringResult(t, "item3", item)
		conn.BRPopLPush("list2", "list", 1 * time.Second)
		t.Log("the list is: ", fmt.Sprintf("%v", conn.LRange("list", 0, -1).Val()))
		conn.BRPopLPush("list", "list2", 1 * time.Second)
		t.Log("the list is: ", fmt.Sprintf("%v", conn.LRange("list", 0, -1).Val()))
		t.Log("the list2 is: ", fmt.Sprintf("%v", conn.LRange("list2", 0, -1).Val()))
		res := conn.BLPop(1 * time.Second, "list", "list2").Val()
		t.Log("the result of blpop: ", res)
		res = conn.BLPop(1 * time.Second, "list", "list2").Val()
		t.Log("the result of blpop: ", res)
		res = conn.BLPop(1 * time.Second, "list", "list2").Val()
		t.Log("the result of blpop: ", res)
		res = conn.BLPop(1 * time.Second, "list", "list2").Val()
		t.Log("the result of blpop: ", res)
		defer conn.Reset()
	})

	t.Run("Operation of set", func(t *testing.T) {
		res := conn.SAdd("set-key", "a", "b", "c").Val()
		assertnumResult(t, 3, res)
		conn.SRem("set-key", "c", "d")
		res = conn.SRem("set-key", "c", "d").Val()
		assertnumResult(t, 0, res)
		res = conn.SCard("set-key").Val()
		assertnumResult(t, 2, 2)
		t.Log("all items in set: ", fmt.Sprintf("%v", conn.SMembers("set-key").Val()))
		conn.SMove("set-key", "set-key2", "a")
		conn.SMove("set-key", "set-key2", "c")
		t.Log("all items in set2: ", fmt.Sprintf("%v", conn.SMembers("set-key2").Val()))

		conn.SAdd("skey1", "a", "b", "c", "d")
		conn.SAdd("skey2", "c", "d", "e", "f")
		set := conn.SDiff("skey1", "skey2").Val()
		t.Log("the diff between two set is: ", set)
		set = conn.SInter("skey1", "skey2").Val()
		t.Log("the inter between two set is: ", set)
		set = conn.SUnion("skey1", "skey2").Val()
		t.Log("the union between two set is: ", set)
		defer conn.Reset()
	})

	t.Run("Operation on hash", func(t *testing.T) {
		conn.HMSet("hash-key", map[string]interface{}{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		})
		res := conn.HMGet("hash-key", "k2", "k3").Val()
		t.Log("the result of get: ", res)
		length := conn.HLen("hash-key").Val()
		assertnumResult(t, 3, length)
		conn.HDel("hash-key", "k1", "k2")
		mps := conn.HGetAll("hash-key").Val()
		t.Log("the result of get: ", mps)
		conn.HMSet("hash-key2", map[string]interface{}{
			"short": "hello",
			"long": "1000",
		})
		strs := conn.HKeys("hash-key2").Val()
		t.Log("the result of hkeys: ", strs)
		isOk := conn.HExists("hash-key2", "num").Val()
		assertFalse(t, isOk)
		count := conn.HIncrBy("hash-key2", "num", 1).Val()
		assertnumResult(t, 1, count)
		defer conn.Reset()
	})

	t.Run("Operation on zset", func(t *testing.T) {
		res := conn.ZAdd("zset-key", redis.Z{Member:"a", Score:3}, redis.Z{Member:"b", Score:2},
		redis.Z{Member:"c", Score:1}).Val()
		assertnumResult(t, 3, res)
		res = conn.ZCard("zset-key").Val()
		assertnumResult(t, 3, res)
		fnum := conn.ZIncrBy("zset-key", 3, "c").Val()
		assertfloatResult(t, 4.0, fnum)
		fnum = conn.ZScore("zset-key", "b").Val()
		assertfloatResult(t, 2.0, fnum)
		res = conn.ZRank("zset-key", "c").Val()
		assertnumResult(t, 2, res)
		res = conn.ZCount("zset-key", "0", "3").Val()
		assertnumResult(t, 2, res)
		conn.ZRem("zset-key", "b")
		zset := conn.ZRangeWithScores("zset-key", 0, -1).Val()
		t.Log("the result of zrange: ", zset)

		conn.ZAdd("zset-1", redis.Z{Member:"a", Score:1}, redis.Z{Member:"b", Score:2},
			redis.Z{Member:"c", Score:3})
		conn.ZAdd("zset-2", redis.Z{Member:"b", Score:4}, redis.Z{Member:"d", Score:0},
			redis.Z{Member:"c", Score:1})
		conn.ZInterStore("zset-i", redis.ZStore{}, "zset-1", "zset-2")
		zset = conn.ZRangeWithScores("zset-i", 0, -1).Val()
		t.Log("the result of zrange: ", zset)
		conn.ZUnionStore("zset-u", redis.ZStore{Aggregate:"min"}, "zset-1", "zset-2")
		zset = conn.ZRangeWithScores("zset-u", 0, -1).Val()
		t.Log("the result of zrange: ", zset)
		conn.SAdd("set-1", "a", "d")
		conn.ZUnionStore("zset-u2", redis.ZStore{}, "zset-1", "zset-2", "set-1")
		zset = conn.ZRangeWithScores("zset-u2", 0, -1).Val()
		t.Log("the result of zrange: ", zset)
		defer conn.Reset()
	})

	t.Run("Sort operation", func(t *testing.T) {
		conn.RPush("sort-input", 23, 15, 110, 7)
		res := conn.Sort("sort-input", &redis.Sort{Order:"ASC"}).Val()
		t.Log("result of sort: ", res)
		res = conn.Sort("sort-input", &redis.Sort{Alpha: true}).Val()
		t.Log("result of sort: ", res)
		conn.HSet("d-7", "field", 5)
		conn.HSet("d-15", "field", 1)
		conn.HSet("d-23", "field", 9)
		conn.HSet("d-110", "field", 3)
		res = conn.Sort("sort-input", &redis.Sort{By:"d-*->field"}).Val()
		t.Log("result of sort: ", res)
		res = conn.Sort("sort-input", &redis.Sort{By:"d-*->field", Get:[]string{"d-*->field"}}).Val()
		t.Log("result of sort: ", res)
		defer conn.Reset()
	})

	t.Run("Set expire", func(t *testing.T) {
		conn.Set("key", "value", 0)
		res := conn.Get("key").Val()
		assertStringResult(t, "value", res)
		conn.Expire("key", 1 * time.Second)
		time.Sleep(2 * time.Second)
		res = conn.Get("key").Val()
		assertStringResult(t, "", res)
		conn.Set("key", "value2", 0)
		conn.Expire("key", 100 * time.Second)
		t.Log("the rest time: ", conn.TTL("key").Val())
		defer conn.Reset()
	})
}


func assertStringResult(t *testing.T, want, get string) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func assertnumResult(t *testing.T, want, get int64) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func assertfloatResult(t *testing.T, want, get float64) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func assertFalse(t *testing.T, v bool) {
	t.Helper()
	if v == true {
		t.Error("assert false but get a true value")
	}
}
