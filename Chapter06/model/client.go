package model

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
	"log"
	"math"
	"redisInAction/Chapter06/common"
	"redisInAction/utils"
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

func (c *Client) AddUpdateContact(user, contact string) {
	acList := "recent:" + user
	pipeline := c.Conn.TxPipeline()
	pipeline.LRem(acList, 1, contact)
	pipeline.LPush(acList, contact)
	pipeline.LTrim(acList, 0, 99)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in AddUpdateContact: ", err)
	}
}

func (c *Client) RemoveContact(user, contact string) {
	c.Conn.LRem("recent:" + user, 1, contact)
}

func (c *Client) FetchAutoCompleteList(user, prefix string) []string {
	candidates := c.Conn.LRange("recent:" + user, 0, -1).Val()
	var matches []string
	for _, candidate := range candidates {
		if strings.HasPrefix(strings.ToLower(candidate), strings.ToLower(prefix)) {
			matches = append(matches, candidate)
		}
	}
	return matches
}

func (c *Client) FindPrefixRange(prefix string) (string, string) {
	posn := strings.IndexByte(common.ValidCharacters, prefix[len(prefix) - 1])
	if posn == 0 {
		posn = 1
	}
	suffix := string(common.ValidCharacters[posn - 1])
	return prefix[: len(prefix) - 1] + suffix + "{", prefix + "{"
}

func (c *Client) AutoCompleteOnPrefix(guild, prefix string) []string {
	start, end := c.FindPrefixRange(prefix)
	identifier := uuid.NewV4().String()
	start += identifier
	end += identifier
	zsetName := "members:" + guild

	var items []string
	c.Conn.ZAdd(zsetName, &redis.Z{Member:start, Score:0}, &redis.Z{Member:end, Score:0})
	for {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			pipeline := tx.TxPipeline()
			sindex := tx.ZRank(zsetName, start).Val()
			eindex := tx.ZRank(zsetName, end).Val()
			erange := utils.Min(sindex + 9, eindex - 2)
			pipeline.ZRem(zsetName, start, end)
			var tmp *redis.StringSliceCmd
			tmp = pipeline.ZRange(zsetName, sindex, erange)
			_, err := pipeline.Exec()
			if err != nil {
				log.Println("pipeline err in AutoCompleteOnPrefix: ", err)
				return err
			}
			res := tmp.Val()
			if len(res) != 0 {
				items = res
			}
			return nil
		}, zsetName)
		if err != nil {
			continue
		}
		break
	}
	var result []string
	for _, item := range items {
		if !strings.Contains(item, "{") {
			result = append(result, item)
		}
	}
	return result
}

func (c *Client) JoinGuild(guild, user string) {
	c.Conn.ZAdd("members:" + guild, &redis.Z{Member:user, Score:0})
}

func (c *Client) LeaveGuild(guild, user string) {
	c.Conn.ZRem("members:" + guild, user)
}

func (c *Client) AcquireLock(lockname string, acquireTimeout float64) string {
	identifier := uuid.NewV4().String()

	end := time.Now().UnixNano() + int64(acquireTimeout * 1e6)
	for time.Now().UnixNano() < end {
		if c.Conn.SetNX("lock:" + lockname, identifier, 0).Val() {
			return identifier
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func (c *Client) PurchaseItemWithLock(buyerId, itemId, sellerId string) bool {
	buyer := fmt.Sprintf("users:%s", buyerId)
	seller := fmt.Sprintf("users:%s", sellerId)
	item := fmt.Sprintf("%s.%s", itemId, sellerId)
	inventory := fmt.Sprintf("inventory:%s", buyerId)

	locked := c.AcquireLock("market:", 10)
	defer c.ReleaseLock("market:", locked)
	if locked == "" {
		return false
	}

	var (
		price float64
		temp string
		funds float64
		err error
	)
	//TODO：怎么样在pipeline中获取值，不使用tx的话
	if err := c.Conn.Watch(func(tx *redis.Tx) error {
		price, err = tx.ZScore("market:", item).Result()
		if err != nil {
			return err
		}
		temp, err = tx.HGet(buyer, "funds").Result()
		funds, _ = strconv.ParseFloat(temp, 64)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Println("tx err in PurchaseItemWithLock: ", err)
	}

	if price == 0 || price > funds {
		return false
	}

	pipe := c.Conn.TxPipeline()
	pipe.HIncrBy(seller, "funds", int64(price))
	pipe.HIncrBy(buyer, "funds", int64(-price))
	pipe.SAdd(inventory, itemId)
	pipe.ZRem("market:", item)
	if _, err := pipe.Exec(); err != nil {
		log.Println("pipeline failed in PurchaseItemWithLock: ", err)
		return false
	}
	return true
}

func (c *Client) ReleaseLock(lockname, identifier string) bool {
	lockname = "lock:" + lockname
	var flag  = true
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

		if ! flag {
			break
		}
	}
	return true
}

func (c *Client) AcquireLockWithTimeout(lockname string, acquireTimeout, lockTimeout float64) string {
	identifier := uuid.NewV4().String()
	lockname = "lock:" + lockname
	finalLockTimeout := math.Ceil(lockTimeout)

	end := time.Now().UnixNano() + int64(acquireTimeout * 1e9)
	for time.Now().UnixNano() < end {
		if c.Conn.SetNX(lockname, identifier, 0).Val() {
			c.Conn.Expire(lockname, time.Duration(finalLockTimeout) * time.Second)
			return identifier
		} else if c.Conn.TTL(lockname).Val() < 0 {
			c.Conn.Expire(lockname, time.Duration(finalLockTimeout) * time.Second)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func (c *Client) AcquireSemaphore(semname string, limit int64, timeout int64) string {
	identifier := uuid.NewV4().String()
	now := time.Now().Unix()

	var res *redis.IntCmd
	pipeline := c.Conn.TxPipeline()
	pipeline.ZRemRangeByScore(semname, "-inf", strconv.Itoa(int(now-timeout)))
	pipeline.ZAdd(semname, &redis.Z{Member:identifier, Score:float64(now)})
	res = pipeline.ZRank(semname, identifier)
	_, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in AcquireSemaphore: ", err)
	}
	if res.Val() < limit {
		return identifier
	}

	c.Conn.ZRem(semname, identifier)
	return ""
}

func (c *Client) ReleaseSemaphore(semname, identifier string) {
	c.Conn.ZRem(semname, identifier)
}

func (c *Client) AcquireFairSemaphore(semname string, limit, timeout int64) string {
	identifier := uuid.NewV4().String()
	czset := semname + ":owner"
	ctr := semname + ":counter"

	now := time.Now().Unix()
	pipeline := c.Conn.TxPipeline()
	pipeline.ZRemRangeByScore(semname, "-inf", strconv.Itoa(int(now-timeout)))
	pipeline.ZInterStore(czset, &redis.ZStore{Keys:[]string{czset, semname}, Weights:[]float64{1, 0}})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in AcquireFairSemaphore: ", err)
		return ""
	}
	counter := c.Conn.Incr(ctr).Val()

	pipeline.ZAdd(semname, &redis.Z{Member:identifier, Score:float64(now)})
	pipeline.ZAdd(czset, &redis.Z{Member:identifier, Score:float64(counter)})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in AcquireFairSemaphore: ", err)
		return ""
	}

	res := c.Conn.ZRank(czset, identifier).Val()
	if res < limit {
		return identifier
	}

	pipeline.ZRem(semname, identifier)
	pipeline.ZRem(czset, identifier)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in AcquireFairSemaphore: ", err)
		return ""
	}
	return ""
}

func (c *Client) ReleaseFairSemaphore(semname, identifier string) bool {
	pipeline := c.Conn.TxPipeline()
	pipeline.ZRem(semname, identifier)
	pipeline.ZRem(semname + ":owner", identifier)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in ReleaseFairSemaphore: ", err)
		return false
	}
	return true
}

func (c *Client) RefreshFairSemaphore(semname, identifier string) bool {
	if c.Conn.ZAdd(semname, &redis.Z{Member:identifier, Score:float64(time.Now().Unix())}).Val() != 0 {
		c.ReleaseFairSemaphore(semname, identifier)
		return false
	}
	return true
}

func (c *Client) AcquireSemaphoreWithLock(semname string, limit int64, timeout int64) string {
	identifier := c.AcquireLock(semname, 0.01)
	if identifier != "" {
		return c.AcquireFairSemaphore(semname, limit, timeout)
	}
	defer c.ReleaseLock(semname, identifier)
	return ""
}

type soldData struct {
	SellerId string
	ItemId string
	Price string
	BuyerId string
	Time int64
}
func (c *Client) SendSoldEmailViaQueue(seller, item, price, buyer string) {
	data := soldData{
		SellerId: seller,
		ItemId:   item,
		Price:    price,
		BuyerId:  buyer,
		Time:     time.Now().Unix(),
	}
	jsonValue, err := json.Marshal(data)
	if err != nil {
		log.Println("marshal err in SendSoldEmailViaQueue: ", err)
		return
	}
	c.Conn.RPush("queue:email", jsonValue)
}

func (c *Client) ProcessSoldEmailQueue() {
	for ! common.QUIT {
		packed := c.Conn.BLPop(30 * time.Second, "queue:email").Val()
		if len(packed) == 0 {
			continue
		}

		toSend := soldData{}
		if err := json.Unmarshal([]byte(packed[0]), &toSend); err != nil {
			log.Println("unmarshal err in ProcessSoldEmailQueue: ", err)
			return
		}

		SendEmail()
	}
}

func SendEmail() {}

type info struct {
	Identifier string
	Queue string
	Name string
	Args []string
}

func (c *Client) ExecuteLater(queue, name string, args []string, delay float64) string {
	identifier := uuid.NewV4().String()
	data := info{
		Identifier: identifier,
		Queue:      queue,
		Name:       name,
		Args:       args,
	}

	item, err := json.Marshal(data)
	if err != nil {
		log.Println("marshal err in ExecuteLater: ", err)
		return ""
	}

	if delay > 0 {
		c.Conn.ZAdd("delayed:", &redis.Z{Member:item, Score:float64(time.Now().UnixNano() + int64(delay * 1e9))})
	} else {
		c.Conn.RPush("queue:" + queue, item)
	}
	return identifier
}

func (c *Client) PollQueue(channel chan struct{}) {
	for ! common.QUIT {
		item := c.Conn.ZRangeWithScores("delayed:", 0, 0).Val()
		if len(item) == 0 || int64(item[0].Score) > time.Now().UnixNano() {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		res := item[0].Member.(string)
		data := info{}
		if err := json.Unmarshal([]byte(res), &data); err != nil {
			log.Println("unmarshal err in PollQueue: ", err)
			channel <- struct{}{}
			return
		}

		locked := c.AcquireLock(data.Identifier, 10)
		if locked == "" {
			continue
		}

		if c.Conn.ZRem("delayed:", res).Val() != 0 {
			c.Conn.RPush("queue:" + data.Queue, res)
		}

		c.ReleaseLock(data.Identifier, locked)
	}

	channel <- struct{}{}
}

func (c *Client) CreateChat(sender string, recipients *[]string, message string, chatId string) string {
	if chatId == "" {
		chatId = strconv.Itoa(int(c.Conn.Incr("ids:chat:").Val()))
	}

	*recipients = append(*recipients, sender)
	var recipientsd []*redis.Z
	for _, r := range *recipients {
		temp := redis.Z{
			Score:  0,
			Member: r,
		}
		recipientsd = append(recipientsd, &temp)
	}

	pipeline := c.Conn.TxPipeline()
	pipeline.ZAdd("chat:" + chatId, recipientsd...)
	for _, rec := range *recipients {
		pipeline.ZAdd("seen:" + rec, &redis.Z{Member:chatId, Score:0})
	}
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in CreateChat: ", err)
	}
	return c.SendMessage(chatId, sender, message)
}

type pack struct {
	Id int64
	Ts int64
	Sender string
	Message string
}

func (c *Client) SendMessage(chatId, sender, message string) string {
	identifier := c.AcquireLock("chat:" + chatId, 10)
	if identifier == "" {
		log.Println("Couldn't get the lock")
		return ""
	}

	mid := c.Conn.Incr("ids:" + chatId).Val()
	ts := time.Now().Unix()
	packed := pack{
		Id:      mid,
		Ts:      ts,
		Sender:  sender,
		Message: message,
	}

	jsonValue, err := json.Marshal(packed)
	if err != nil {
		log.Println("marshal err in SendMessage: ", err)
	}

	c.Conn.ZAdd("msgs:" + chatId, &redis.Z{Member:jsonValue, Score:float64(mid)})
	defer c.ReleaseLock("chat:" + chatId, identifier)
	return chatId
}

//TODO: 怎么实现 FetchPendingMessage
//func (c *Client) FetchPendingMessage(recipient string) {
//	seen := c.Conn.ZRangeWithScores("seen:" + recipient, 0, -1).Val()
//	pipeline := c.Conn.TxPipeline()
//
//	var res *redis.StringSliceCmd
//	length := len(seen)
//	temp := make([]string, 0, length)
//	for _, v := range seen {
//		chatId := v.Member.(string)
//		seenId := v.Score
//		res = pipeline.ZRangeByScore("msgs:" + chatId, &redis.ZRangeBy{Min:strconv.Itoa(int(seenId + 1)), Max:"inf"})
//		temp = append(temp, chatId, strconv.Itoa(int(seenId)))
//	}
//
//	if _, err := pipeline.Exec(); err != nil {
//		log.Println("pipeline err in FetchPendingMessage: ", err)
//		return
//	}
//	chatInfo := [][]string{temp, res.Val()}
//
//	for i, v := range chatInfo {
//
//	}
//
//
//}