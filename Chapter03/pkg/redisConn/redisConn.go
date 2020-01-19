package redisConn

import (
	"github.com/go-redis/redis"
	"log"
	"redisInAction/Chapter03/pkg/common"
	"redisInAction/config"
	"redisInAction/utils"
	"strings"
	"time"
)

type RedisClient redis.Client

func ConnectRedis() *RedisClient {
	conn := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})

	if _, err := conn.Ping().Result(); err != nil {
		log.Fatalf("Connect to redis client failed, err: %v\n", err)
	}
	return (*RedisClient)(conn)
}

func (r *RedisClient) Reset() {
	delKeys := []string{"key", "new-string-key", "another-key", "list-key", "list", "list2", "set-key", "set-key2",
		"skey1", "skey2", "hash-key", "hash-key2", "zset-key", "zset-1", "zset-2", "zset-i", "zset-u", "zset-u2",
		"set-1", "sort-input", "d-*", "notrans: ", "trans:", "key"}
	var toDel []string
	for _, v := range delKeys {
		toDel = append(toDel, r.Keys(v).Val()...)
	}

	if len(toDel) != 0 {
		r.Del(toDel...)
	}
}

//=========================== the answer of exercise ==================================
func (r *RedisClient) UpdateToken(token, user, item string) {
	timestamp := time.Now().Unix()
	r.HSet("login:", token, user)
	r.ZAdd("recent", redis.Z{Score:float64(timestamp), Member:token})
	if item != "" {
		key := "viewed" + token
		r.LRem(key, 1, item)
		r.RPush(key, item)
		r.LTrim(key, -25, -1)
		r.ZIncrBy("viewed:", -1, item)
	}
}

//TODO:看python代码，有更优的方式
func (r *RedisClient) ArticleVote(article, user string) {
	conn := (*redis.Client)(r)
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	posted := conn.ZScore("time", article).Val()
	if posted < float64(cutoff) {
		return
	}

	articleId := strings.Split(article, ":")[1]
	pipeline := conn.Pipeline()
	pipeline.SAdd("voted:"+articleId, user)
	pipeline.Expire("voted:" + articleId, time.Duration(int(posted-float64(cutoff))) * time.Second)
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline failed, the err is: ", err)
	}
	if res[0] != nil {
		pipeline.ZIncrBy("score:", common.VoteScore, article)
		r.HIncrBy(article, "votes", 1)
		if _, err := pipeline.Exec(); err != nil {
			log.Println("pipeline failed, the err is: ", err)
		}
	}
}

//TODO: 待实现
func (r *RedisClient) GetArticles(page int64, order string) []map[string]string {
	if order == "" {
		order = "score:"
	}
	start := utils.Max(page - 1, 0) * common.ArticlesPerPage
	end := start + common.ArticlesPerPage - 1

	ids := r.ZRevRange(order, start, end).Val()
	articles := []map[string]string{}
	for _, id := range ids {
		articleData := r.HGetAll(id).Val()
		articleData["id"] = id
		articles = append(articles, articleData)
	}
	return articles
}