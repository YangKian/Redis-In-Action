package model

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
	"log"
	"math"
	"redisInAction/Chapter05/common"
	"redisInAction/utils"
	"sort"
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

func (c *Client) LogRecent(name, message, severity string, pipeliner redis.Pipeliner) {
	if severity == "" {
		severity = "INFO"
	}
	destination := fmt.Sprintf("recent:%s:%s", name, severity)
	message = time.Now().Local().String() + " " + message

	if pipeliner == nil {
		pipeliner = c.Conn.Pipeline()
	}
	pipeliner.LPush(destination, message)
	pipeliner.LTrim(destination, 0, 90)
	if _, err := pipeliner.Exec(); err != nil {
		log.Println("LogRecent pipline err: ", err)
	}
}

func (c *Client) LogCommon(name, message, severity string, timeout int64) {
	destination := fmt.Sprintf("common:%s:%s", name, severity)
	startKey := destination + ":start"
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)

	for time.Now().Before(end) {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			hourStart := time.Now().Local().Hour()
			existing, _ := strconv.Atoi(tx.Get(startKey).Val())

			if _, err := tx.Pipelined(func(pipeliner redis.Pipeliner) error {
				if existing != 0 && existing < hourStart {
					pipeliner.Rename(destination, destination+":last")
					pipeliner.Rename(startKey, destination+":pstart")
					pipeliner.Set(startKey, hourStart, 0)
				} else if existing == 0 {
					pipeliner.Set(startKey, hourStart, 0)
				}

				pipeliner.ZIncrBy(destination, 1, message)
				c.LogRecent(name, message, severity, pipeliner)
				return nil
			}); err != nil {
				log.Println("LogCommon pipelined failed, err: ", err)
				return err
			}
			return nil
		}, startKey)
		if err != nil {
			log.Println("watch failed, err: ", err)
			continue
		}
	}
}

func (c *Client) UpdateCounter(name string, count int64, now int64) {
	if now == 0 {
		now = time.Now().Unix()
	}

	pipe := c.Conn.Pipeline()
	for _, prec := range common.PRECISION {
		pnow := (now / prec) * prec
		hash := fmt.Sprintf("%d:%s", prec, name)
		pipe.ZAdd("known:", &redis.Z{Member: hash, Score: 0})
		pipe.HIncrBy("count:"+hash, strconv.Itoa(int(pnow)), count)
	}
	if _, err := pipe.Exec(); err != nil {
		log.Println("updateCounter err: ", err)
	}
}

func (c *Client) GetCount(name, precision string) [][]int {
	hash := fmt.Sprintf("%v:%s", precision, name)
	data := c.Conn.HGetAll("count:" + hash).Val()
	toReturn := make([][]int, 0, len(data))
	for k, v := range data {
		temp := make([]int, 2)
		key, _ := strconv.Atoi(k)
		num, _ := strconv.Atoi(v)
		temp[0], temp[1] = key, num
		toReturn = append(toReturn, temp)
	}
	sort.Slice(toReturn, func(i, j int) bool {
		return toReturn[i][0] < toReturn[j][0]
	})
	return toReturn
}

func (c *Client) CleanCounters() {
	passes := 0

	for !common.QUIT {
		start := time.Now().Unix()
		var index int64 = 0
		for index < c.Conn.ZCard("known:").Val() {
			hash := c.Conn.ZRange("known:", index, index).Val()
			index++
			if len(hash) == 0 {
				break
			}

			hashValue := hash[0]
			prec, _ := strconv.Atoi(strings.Split(hashValue, ":")[0])
			bprec := prec / 60
			if bprec == 0 {
				bprec = 1
			}
			if passes%bprec != 0 {
				continue
			}

			hkey := "count:" + hashValue
			cutoff := int(time.Now().Unix()) - common.SAMPLECOUNT*prec
			samples := c.Conn.HKeys(hkey).Val()
			sort.Slice(samples, func(i, j int) bool {
				return samples[i] < samples[j]
			})
			remove := sort.SearchStrings(samples, strconv.Itoa(cutoff))

			if remove != 0 {
				c.Conn.HDel(hkey, samples[:remove]...)
				if remove == len(samples) {
					err := c.Conn.Watch(func(tx *redis.Tx) error {
						if tx.HLen(hkey).Val() == 0 {
							pipe := tx.Pipeline()
							pipe.ZRem("known:", hashValue)
							_, err := pipe.Exec()
							if err != nil {
								log.Println("pipeline failed in CleanCounters: ", err)
								return err
							}
							index--
						} else {
							tx.Unwatch()
						}
						return nil
					}, hkey)

					if err != nil {
						log.Println("watch err in CleanCounters: ", err)
						continue
					}
				}
			}
		}
		passes++
		duration := utils.Min(time.Now().Unix()-start+1, 60)
		time.Sleep(time.Duration(utils.Max(60-duration, 1)) * time.Minute)
	}
}

func (c *Client) UpdateStats(context, types string, value float64, timeout int64) []redis.Cmder {
	destination := fmt.Sprintf("stats:%s:%s", context, types)
	startKey := destination + ":start"
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)

	var res []redis.Cmder
	for time.Now().Before(end) {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			hourStart := time.Now().Local().Hour()
			existing, _ := strconv.Atoi(tx.Get(startKey).Val())

			if _, err := tx.Pipelined(func(pipeliner redis.Pipeliner) error {
				if existing == 0 {
					pipeliner.Set(startKey, hourStart, 0)
				} else if existing < hourStart {
					pipeliner.Rename(destination, destination + ":last")
					pipeliner.Rename(startKey, destination + ":pstart")
					pipeliner.Set(startKey, hourStart, 0)
				}

				tkey1 := uuid.NewV4().String()
				tkey2 := uuid.NewV4().String()
				pipeliner.ZAdd(tkey1, &redis.Z{Member:"min", Score:value})
				pipeliner.ZAdd(tkey2, &redis.Z{Member:"max", Score:value})
				pipeliner.ZUnionStore(destination, &redis.ZStore{Aggregate:"MIN", Keys:[]string{destination, tkey1}})
				pipeliner.ZUnionStore(destination, &redis.ZStore{Aggregate:"MAX", Keys:[]string{destination, tkey2}})

				pipeliner.Del(tkey1, tkey2)
				pipeliner.ZIncrBy(destination, 1, "count")
				pipeliner.ZIncrBy(destination, value, "sum")
				pipeliner.ZIncrBy(destination, value * value, "sumq")
				res, _ = pipeliner.Exec()
				res = res[len(res) - 3: ]
				return nil
			}); err != nil {
				log.Println("pipeline filed in UpdateStats: ", err)
				return err
			}
			return nil
		}, startKey)

		if err != nil {
			log.Println("watch filed in UpdateStats: ", err)
			continue
		}
	}
	return res
}

func (c *Client) GetStats(context, types string) map[string]float64 {
	key := fmt.Sprintf("stats:%s:%s", context, types)
	stats := map[string]float64{}
	data := c.Conn.ZRangeWithScores(key, 0, -1).Val()
	for _, v := range data {
		stats[v.Member.(string)] = v.Score
	}
	stats["average"] = stats["sum"] / stats["count"]
	numerator := stats["sumq"] - (math.Pow(stats["sum"], 2) / stats["count"])
	count := stats["count"]
	if count > 1 {
		count--
	} else {
		count = 1
	}
	stats["stddev"] = math.Pow(numerator / count, 0.5)
	return stats
}

//TODO：装饰器实现上下文管理器


