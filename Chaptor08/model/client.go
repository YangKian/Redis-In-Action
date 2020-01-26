package model

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
	"log"
	"math"
	"redisInAction/Chaptor08/common"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}

func (c *Client) CreateUser(login, name string) string {
	llogin := strings.ToLower(login)
	lock := c.AcquireLockWithTimeout("user:" + llogin, 10, 10)
	defer c.ReleaseLock("user:" + llogin, lock)

	if lock == "" {
		return ""
	}

	if c.Conn.HGet("users:", llogin).Val() != "" {
		//c.ReleaseLock("user:" + llogin, lock)
		return ""
	}

	id := c.Conn.Incr("user:id:").Val()

	pipeline := c.Conn.TxPipeline()
	pipeline.HSet("users:", llogin, id)
	pipeline.HMSet(fmt.Sprintf("user:%s", strconv.Itoa(int(id))), "login", login,
		"id", id, "name", name, "followers", 0, "following", 0, "posts", 0,
		"signup", time.Now().Unix())
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in CreateUser: ", err)
		return ""
	}
	return strconv.Itoa(int(id))
}

func (c *Client) CreateStatus(uid, message string, data map[string]interface{}) string {
	pipeline := c.Conn.TxPipeline()
	pipeline.HGet(fmt.Sprintf("user:%s", uid), "login")
	pipeline.Incr("status:id:")
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in CreateStatus: ", err)
		return ""
	}
	login, id := res[0].(*redis.StringCmd).Val(), res[1].(*redis.IntCmd).Val()

	if login == "" {
		return ""
	}

	data["message"] = message
	data["posted"] = time.Now().Unix()
	data["id"] = id
	data["uid"] = uid
	data["login"] = login

	pipeline.HMSet(fmt.Sprintf("status:%s", id), data)
	pipeline.HIncrBy(fmt.Sprintf("user:%s", uid), "posts", 1)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in CreateStatus: ", err)
		return ""
	}
	return strconv.Itoa(int(id))
}

func (c *Client) PostStatus(uid, message string, data map[string]interface{}) string {
	id := c.CreateStatus(uid, message, data)
	if id == "" {
		return ""
	}

	posted, err := c.Conn.HGet(fmt.Sprintf("status:%s", id), "posted").Float64()
	if err != nil {
		log.Printf("hget from status:%s err: %v\n", id, err)
		return ""
	}
	if posted == 0 {
		return ""
	}

	post := map[string]float64{id: posted}
	c.Conn.ZAdd(fmt.Sprintf("profile:%s", uid), &redis.Z{Member:id, Score:posted})

	c.syndicateStatus(uid, post, 0)
	return id
}

func (c *Client) syndicateStatus(uid string, post map[string]float64, start int) {
	followers := c.Conn.ZRangeByScoreWithScores(fmt.Sprintf("followers:%s", uid),
		&redis.ZRangeBy{Min:strconv.Itoa(start), Max:"inf", Offset:0, Count:common.POSTPERPASS}).Val()

	pipeline := c.Conn.TxPipeline()
	for _, z := range followers {
		follower := z.Member.(string)
		start = int(z.Score)
		pipeline.ZAdd(fmt.Sprintf("home:%s", follower), &redis.Z{Member:follower, Score:post[follower]})
		pipeline.ZRemRangeByRank(fmt.Sprintf("home:%s", follower), 0, -common.HOMETIMELINESIZE - 1)
	}
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in syndicateStatus: ", err)
		return
	}

	if len(followers) >= common.POSTPERPASS {
		c.executeLater("default", "syndicateStatus", uid, post, start)
	}
}

//TODO：怎么用守护线程的模式来实现
func (c *Client) executeLater(queue, name string, args ...interface{}) {
	go func() {
		methodValue := reflect.ValueOf(c.Conn).MethodByName(name)
		methodArgs := make([]reflect.Value, 0, len(args))
		for _, arg := range args {
			args = append(args, reflect.ValueOf(arg))
		}
		methodValue.Call(methodArgs)
	}()
}

func (c *Client) GetStatusMessage(uid, timeline string, page, count int64) []map[string]string {
	statuses := c.Conn.ZRevRange(fmt.Sprintf("%s%s", timeline, uid),
		(page - 1) * count, page * count - 1).Val()
	pipeline := c.Conn.TxPipeline()
	for _, id := range statuses {
		pipeline.HGetAll(fmt.Sprintf("status:%s", id))
	}
	res, err := pipeline.Exec()
	if err != nil {
		return nil
	}
	final := make([]map[string]string, 0, len(res))
	for _, val := range res {
		temp := val.(*redis.StringStringMapCmd).Val()
		if temp != nil {
			final = append(final, temp)
		}
	}
	return final
}

func (c *Client) FollowUser(uid, otherUid string) bool {
	fkey1 := fmt.Sprintf("following:%s", uid)
	fkey2 := fmt.Sprintf("followers:%s", otherUid)

	if c.Conn.ZScore(fkey1, otherUid).Val() != 0 {
		return false
	}

	now := time.Now().Unix()

	pipeline := c.Conn.TxPipeline()
	pipeline.ZAdd(fkey1, &redis.Z{Member:otherUid, Score:float64(now)})
	pipeline.ZAdd(fkey2, &redis.Z{Member:uid, Score:float64(now)})
	pipeline.ZRevRangeWithScores(fmt.Sprintf("profile:%s", otherUid), 0, common.HOMETIMELINESIZE - 1)
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in FollowUser")
	}
	following, followers, statusAndScore :=
		res[0].(*redis.IntCmd).Val(), res[1].(*redis.IntCmd).Val(), res[2].(*redis.ZSliceCmd).Val()

	pipeline.HIncrBy(fmt.Sprintf("user:%s", uid), "following", following)
	pipeline.HIncrBy(fmt.Sprintf("user:%s", otherUid), "followers", followers)
	if statusAndScore != nil {
		for _, z := range statusAndScore {
			pipeline.ZAdd(fmt.Sprintf("home:%s", uid), &z)
		}
	}
	pipeline.ZRemRangeByRank(fmt.Sprintf("home:%s", uid), 0, -common.HOMETIMELINESIZE - 1)

	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in FollowUser")
		return false
	}
	return true
}

func (c *Client) UnfollowUser(uid, otherUid string) bool {
	fkey1 := fmt.Sprintf("following:%s", uid)
	fkey2 := fmt.Sprintf("followers:%s", otherUid)

	if c.Conn.ZScore(fkey1, otherUid).Val() == 0 {
		return false
	}

	pipeline := c.Conn.TxPipeline()
	pipeline.ZRem(fkey1, otherUid)
	pipeline.ZRem(fkey2, uid)
	pipeline.ZRevRange(fmt.Sprintf("profile:%s", otherUid), 0, common.HOMETIMELINESIZE - 1)
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in FollowUser")
	}
	following, followers, status :=
		res[0].(*redis.IntCmd).Val(), res[1].(*redis.IntCmd).Val(), res[2].(*redis.StringSliceCmd).Val()

	pipeline.HIncrBy(fmt.Sprintf("user:%s", uid), "following", -following)
	pipeline.HIncrBy(fmt.Sprintf("user:%s", otherUid), "followers", -followers)
	if len(status) != 0 {
		pipeline.ZRem(fmt.Sprintf("home:%s", uid), status)
	}

	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in UnfollowUser")
		return false
	}
	return true
}

func (c *Client) RefillTimeline(incoming, timeline string, start int) {
	if start == 0 && c.Conn.ZCard(timeline).Val() >= 750 {
		return
	}

	users := c.Conn.ZRangeByScoreWithScores(incoming,
		&redis.ZRangeBy{Min:strconv.Itoa(start), Max:"inf", Offset:0, Count:common.REFILLUSERSSTEP}).Val()

	pipeline := c.Conn.TxPipeline()
	for _, z := range users {
		pipeline.ZRevRangeWithScores(fmt.Sprintf("profils:%s", z.Member),
			int64(z.Score), common.HOMETIMELINESIZE -1)
	}


	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in RefillTimeline", err)
		return
	}
	message := make([]string, 0, len(res))
	for _, z := range res {
		value := z.(*redis.StringCmd)
		message = append(message, value.Val())
	}

}

func (c *Client) AcquireLockWithTimeout(lockname string, acquireTimeout, lockTimeout float64) string {
	identifier := uuid.NewV4().String()
	lockname = "lock:" + lockname
	finalLockTimeout := math.Ceil(lockTimeout)

	end := time.Now().UnixNano() + int64(acquireTimeout*1e9)
	for time.Now().UnixNano() < end {
		if c.Conn.SetNX(lockname, identifier, 0).Val() {
			c.Conn.Expire(lockname, time.Duration(finalLockTimeout)*time.Second)
			return identifier
		} else if c.Conn.TTL(lockname).Val() < 0 {
			c.Conn.Expire(lockname, time.Duration(finalLockTimeout)*time.Second)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func (c *Client) ReleaseLock(lockname, identifier string) bool {
	lockname = "lock:" + lockname
	var flag = true
	for flag {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			pipe := tx.TxPipeline()
			if tx.Get(lockname).Val() == identifier {
				pipe.Del(lockname)
				if _, err := pipe.Exec(); err != nil {
					return err
				}
				flag = true
				return nil
			}

			tx.Unwatch()
			flag = false
			return nil
		})

		if err != nil {
			log.Println("watch failed in ReleaseLock, err is: ", err)
			return false
		}

		if !flag {
			break
		}
	}
	return true
}
