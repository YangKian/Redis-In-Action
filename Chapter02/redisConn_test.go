package main

import (
	"github.com/satori/go.uuid"
	"redisInAction/Chapter02/pkg/common"
	"redisInAction/Chapter02/pkg/redisConn"
	"testing"
	"time"
)

func TestLoginCookies(t *testing.T) {
	conn := redisConn.ConnectRedis()

	token := uuid.NewV4().String()

	t.Run("Test UpdateToken", func(t *testing.T) {
		conn.UpdateToken(token, "username", "itemX")
		t.Log("We just logged-in/update token: \n", token)
		t.Log("For user: ", "username\n")
		t.Log("\nWhat username do we get when we look-up that token?\n")
		r := conn.CheckToken(token)
		t.Log("username: ", r)
		assertStringResult(t, "username", r)

		t.Log("Let's drop the maximum number of cookies to 0 to clean them out\n")
		t.Log("We will start a thread to do the cleaning, while we stop it later\n")

		common.LIMIT = 0
		go conn.CleanSessions()
		time.Sleep(1 * time.Second)
		common.QUIT = true
		time.Sleep(2 * time.Second)

		assertThread(t, common.FLAG)

		s := conn.HLen("login:").Val()
		t.Log("The current number of sessions still available is:", s)
		assertnumResult(t, 1, int(s))
		defer conn.Reset()
	})

	t.Run("Test shopping cart cookie", func(t *testing.T) {
		t.Log("We'll refresh our session...")
		conn.UpdateToken(token, "username", "itemX")
		t.Log("And add an item to the shopping cart")
		conn.AddToCart(token, "itemY", 3)
		r := conn.HGetAll("cart:" + token).Val()
		t.Log("Our shopping cart currently has:", r)

		assertTrue(t, len(r) >= 1)

		t.Log("Let's clean out our sessions and carts")
		common.LIMIT = 0
		go conn.CleanFullSession()
		time.Sleep(1 * time.Second)
		common.QUIT = true
		time.Sleep(2 * time.Second)
		assertThread(t, common.FLAG)

		r = conn.HGetAll("cart:" + token).Val()
		t.Log("Our shopping cart now contains:", r)
		defer conn.Reset()
	})

	//TODO：后续请求相关的部分未做
	t.Run("Test cache request", func(t *testing.T) {
		
	})
}

func assertThread(t *testing.T, threadStat int32) {
	if threadStat != 0 {
		t.Error("The clean sessions thread is still alive?!?")
	}
}

func assertStringResult(t *testing.T, want, get string) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func assertnumResult(t *testing.T, want, get int) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func assertTrue(t *testing.T, v bool) {
	t.Helper()
	if v != true {
		t.Error("assert true but get a false value")
	}
}
