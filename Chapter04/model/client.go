package model

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"log"
	"time"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}

func (c *Client) ListItem(itemid, sellerid string, price float64) bool {
	inventory := fmt.Sprintf("inventory:%s", sellerid)
	item := fmt.Sprintf("%s.%s", itemid, sellerid)
	end := time.Now().Unix() + 5

	for time.Now().Unix() < end {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			if _, err := tx.TxPipelined(func(pipeliner redis.Pipeliner) error {
				if !tx.SIsMember(inventory, itemid).Val() {
					tx.Unwatch(inventory)
					return nil
				}
				pipeliner.ZAdd("market:", &redis.Z{Member: item, Score: price})
				pipeliner.SRem(inventory, itemid)
				return nil
			}); err != nil {
				return err
			}
			return nil
		}, inventory)

		if err != nil {
			log.Println("watch err: ", err)
			return false
		}
		return true
	}
	return false
}


func (c *Client) PurchaseItem(buyerid, itemid, sellerid string, lprice int64) bool {
	buyer := fmt.Sprintf("users:%s", buyerid)
	seller := fmt.Sprintf("users:%s", sellerid)
	item := fmt.Sprintf("%s.%s", itemid, sellerid)
	inventory := fmt.Sprintf("inventory:%s", buyerid)
	end := time.Now().Unix() + 10

	for time.Now().Unix() < end {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			if _, err := tx.TxPipelined(func(pipeliner redis.Pipeliner) error {
				price := int64(pipeliner.ZScore("market:", item).Val())
				funds, _ := tx.HGet(buyer, "funds").Int64()
				if price != lprice || price > funds {
					tx.Unwatch()
				}

				pipeliner.HIncrBy(seller, "funds", price)
				pipeliner.HIncrBy(buyer, "funds", -price)
				pipeliner.SAdd(inventory, itemid)
				pipeliner.ZRem("market:", item)
				return nil
			}); err != nil {
				return err
			}
			return nil
		}, "market:", buyer)
		if err != nil {
			log.Println(err)
			return false
		}
		return true
	}
	return false
}

//TODO:4.4之前的部分未实现

func (c *Client) ProcessLogs(path string, callback func(string)) {
	//currentFile := c.Conn.MGet("progress:file").Val()[0].(string)
	//offset := c.Conn.MGet("progress:position").Val()[0].(int)
	//
	//pipeline := c.Conn.Pipeline()
	//
	//files, err := ioutil.ReadDir(path)
	//if err != nil {
	//	log.Fatalf("read path failed, err: %v\n", err)
	//}
	//for _, fname := range files {
	//	if fname.Name() < currentFile {
	//		continue
	//	}
	//
	//	inp, err := os.OpenFile(filepath.Join(path, fname.Name()), os.O_RDONLY, 0)
	//	if err != nil {
	//		log.Fatalln("open file failed, err: ", err)
	//	}
	//	//offsetInt, _ := strconv.ParseInt(offset, 10, 64)
	//	if fname.Name() == currentFile {
	//		if _, err := inp.Seek(int64(offset), 10); err != nil {
	//			log.Fatalln("Seek failed, err is: ", err)
	//		}
	//	} else {
	//		offset = 0
	//	}
	//
	//	currentFile = ""
	//
	//	for lno, line := range inp.
	//}

}
