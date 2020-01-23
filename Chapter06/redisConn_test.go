package main

import (
	"fmt"
	"redisInAction/Chapter06/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"reflect"
	"strings"
	"testing"
	"time"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)
	
	t.Run("Test add update contact", func(t *testing.T) {
		t.Log("Let's add a few contacts...")
		for i := 0; i < 10; i++ {
			client.AddUpdateContact("user", fmt.Sprintf("contact-%d-%d", i / 3, i))
		}
		t.Log("Current recently contacted contacts")
		contacts := client.Conn.LRange("recent:user", 0, -1).Val()
		for _, v := range contacts {
			t.Log(v)
		}
		utils.AssertTrue(t, len(contacts) >= 10)

		t.Log("Let's pull one of the older ones up to the front")
		client.AddUpdateContact("user", "contact-1-4")
		contacts = client.Conn.LRange("recent:user", 0, 2).Val()
		t.Log("New top-3 contacts:")
		for _, v := range contacts {
			t.Log(v)
		}
		utils.AssertTrue(t, contacts[0] == "contact-1-4")

		t.Log("Let's remove a contact...")
		client.RemoveContact("user", "contact-2-6")
		contacts = client.Conn.LRange("recent:user", 0, -1).Val()
		t.Log("New contacts:")
		for _, v := range contacts {
			t.Log(v)
		}
		utils.AssertTrue(t, len(contacts) >= 9)

		t.Log("And let's finally autocomplete on ")
		all := client.Conn.LRange("recent:user", 0, -1).Val()
		contacts = client.FetchAutoCompleteList("user", "c")
		utils.AssertTrue(t, reflect.DeepEqual(all, contacts))
		equiv := []string{}
		for _, v := range all {
			if strings.HasPrefix(v, "contact-2-") {
				equiv = append(equiv, v)
			}
		}
		contacts = client.FetchAutoCompleteList("user", "contact-2-")
		utils.AssertTrue(t, reflect.DeepEqual(equiv, contacts))
		defer client.Conn.FlushAll()
	})

	t.Run("Test address book autocomplete", func(t *testing.T) {
		t.Log("the start/end range of 'abc' is:",)
		start, end := client.FindPrefixRange("abc")
		t.Log(start, end)

		t.Log("Let's add a few people to the guild")
		for _, name := range []string{"jeff", "jenny", "jack", "jennifer"} {
			client.JoinGuild("test", name)
		}
		t.Log("now let's try to find users with names starting with 'je':")
		r := client.AutoCompleteOnPrefix("test", "je")
		t.Log(r)
		defer client.Conn.FlushAll()
	})

	t.Run("Test distributed locking", func(t *testing.T) {
		t.Log("Getting an initial lock...")
		utils.AssertTrue(t, client.AcquireLockWithTimeout("testlock", 1, 1) != "")
		t.Log("Got it!")
		t.Log("Trying to get it again without releasing the first one...")
		utils.AssertFalse(t, client.AcquireLockWithTimeout("testlock", 0.01, 1) != "")
		t.Log("Failed to get it!")
		t.Log("Waiting for the lock to timeout...")
		time.Sleep(1 * time.Second)
		t.Log("Getting the lock again...")
		r := client.AcquireLockWithTimeout("testlock", 1, 1)
		utils.AssertTrue(t, r != "")
		t.Log("Got it!")
		t.Log("Releasing the lock...")
		utils.AssertTrue(t, client.ReleaseLock("testlock", r))
		t.Log("Released it...")
		t.Log("Acquiring it again...")
		utils.AssertTrue(t, client.AcquireLockWithTimeout("testlock", 1, 1) != "")
		t.Log("Got it!")
		defer client.Conn.FlushAll()
	})

	t.Run("Test counting semaphore", func(t *testing.T) {
		t.Log("Getting 3 initial semaphores with a limit of 3...")
		for i := 0; i < 3; i++ {
			utils.AssertTrue(t, client.AcquireFairSemaphore("testsem", 3, 1) != "")
		}
		t.Log("Done!")
		t.Log("Getting one more that should fail...")
		utils.AssertFalse(t, client.AcquireFairSemaphore("testsem", 3, 1) != "")
		t.Log("Couldn't get it!")
		t.Log("Lets's wait for some of them to time out")
		time.Sleep(2 * time.Second)
		t.Log("Can we get one?")
		r := client.AcquireFairSemaphore("testsem", 3, 1)
		utils.AssertTrue(t, r != "")
		t.Log("Got one!")
		t.Log("Let's release it...")
		utils.AssertTrue(t, client.ReleaseFairSemaphore("testsem", r))
		t.Log("Released!")
		t.Log("And let's make sure we can get 3 more!")
		for i := 0; i < 3; i++ {
			utils.AssertTrue(t, client.AcquireFairSemaphore("testsem", 3, 1) != "")
		}
		t.Log("We got them!")
		defer client.Conn.FlushAll()
	})
}


