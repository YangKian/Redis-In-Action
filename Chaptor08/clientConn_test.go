package main

import (
	"redisInAction/Chaptor08/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"testing"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	t.Run("Test create user and status", func(t *testing.T) {
		utils.AssertTrue(t, client.CreateUser("TestUser", "Test User") == "1")
		utils.AssertTrue(t, client.CreateUser("TestUser", "Test User2") == "")

		utils.AssertTrue(t, client.CreateStatus("1", "This is a new status message",
			map[string]interface{}{}) == "1")
		utils.AssertTrue(t, client.Conn.HGet("user:1", "posts").Val() == "1")
		defer client.Conn.FlushAll()
	})
	
	t.Run("Test follow and unfollow user", func(t *testing.T) {
		utils.AssertTrue(t, client.CreateUser("TestUser", "Test User") == "1")
		utils.AssertTrue(t, client.CreateUser("TestUser2", "Test User2") == "2")

		utils.AssertTrue(t, client.FollowUser("1", "2"))
		utils.AssertTrue(t, client.Conn.ZCard("followers:2").Val() == 1)
		utils.AssertTrue(t, client.Conn.ZCard("followers:1").Val() == 0)
		utils.AssertTrue(t, client.Conn.ZCard("following:1").Val() == 1)
		utils.AssertTrue(t, client.Conn.ZCard("following:2").Val() == 0)
		utils.AssertTrue(t, client.Conn.HGet("user:1", "following").Val() == "1")
		utils.AssertTrue(t, client.Conn.HGet("user:2", "following").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:1", "followers").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:2", "followers").Val() == "1")

		utils.AssertFalse(t, client.UnfollowUser("2", "1"))
		utils.AssertTrue(t, client.UnfollowUser("1", "2"))
		utils.AssertTrue(t, client.Conn.ZCard("followers:2").Val() == 0)
		utils.AssertTrue(t, client.Conn.ZCard("followers:1").Val() == 0)
		utils.AssertTrue(t, client.Conn.ZCard("following:1").Val() == 0)
		utils.AssertTrue(t, client.Conn.ZCard("following:2").Val() == 0)
		utils.AssertTrue(t, client.Conn.HGet("user:1", "following").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:2", "following").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:1", "followers").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:2", "followers").Val() == "0")
		defer client.Conn.FlushAll()
	})

	t.Run("Test syndicate status", func(t *testing.T) {
		utils.AssertTrue(t, client.CreateUser("TestUser", "Test User") == "1")
		utils.AssertTrue(t, client.CreateUser("TestUser2", "Test User2") == "2")
		utils.AssertTrue(t, client.FollowUser("1", "2"))
		utils.AssertTrue(t, client.Conn.ZCard("followers:2").Val() == 1)
		utils.AssertTrue(t, client.Conn.HGet("user:1", "following").Val() == "1")
		utils.AssertTrue(t, client.PostStatus("2", "this is some messages content",
			map[string]interface{}{}) == "1")
		utils.AssertnumResult(t, 1, int64(len(client.GetStatusMessage("1", "home", 1, 30))))
		client.Conn.FlushAll()

	})
}