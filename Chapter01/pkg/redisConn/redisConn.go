package redisConn

import (
	"github.com/go-redis/redis"
	"log"
	"redisInAction/Chapter01/pkg/common"
	"redisInAction/config"
	"strconv"
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

func (r *RedisClient) ArticleVote(article, user string) {
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	if r.ZScore("time", article).Val() < float64(cutoff) {
		return
	}

	articleId := strings.Split(article, ":")[1]
	if r.SAdd("voted:"+articleId, user).Val() != 0 {
		r.ZIncrBy("score:", common.VoteScore, article)
		r.HIncrBy(article, "votes", 1)
	}
}

func (r *RedisClient) PostArticle(user, title, link string) string {
	articleId := strconv.Itoa(int(r.Incr("article:").Val()))

	voted := "voted:" + articleId
	r.SAdd(voted, user)
	r.Expire(voted, common.OneWeekInSeconds*time.Second)

	now := time.Now().Unix()
	article := "article:" + articleId
	r.HMSet(article, map[string]interface{}{
		"title":  title,
		"link":   link,
		"poster": user,
		"time":   now,
		"votes":  1,
	})

	r.ZAdd("score:", redis.Z{Score: float64(now + common.VoteScore), Member: article})
	r.ZAdd("time", redis.Z{Score: float64(now), Member: article})
	return articleId
}

func (r *RedisClient) GetArticles(page int64, order string) []map[string]string {
	if order == "" {
		order = "score:"
	}
	start := (page - 1) * common.ArticlesPerPage
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

func (r *RedisClient) AddRemoveGroups(articleId string, toAdd, toRemove []string) {
	article := "article:" + articleId
	for _, group := range toAdd {
		r.SAdd("group:"+group, article)
	}
	for _, group := range toRemove {
		r.SRem("group:"+group, article)
	}
}

func (r *RedisClient) GetGroupArticles(group, order string, page int64) []map[string]string {
	if order == "" {
		order = "score:"
	}
	key := order + group
	if r.Exists(key).Val() == 0 {
		res := r.ZInterStore(key, redis.ZStore{Aggregate: "MAX"}, "group:"+group, order).Val()
		if res <= 0 {
			log.Println("ZInterStore return 0")
		}
	}
	r.Expire(key, 60*time.Second)
	return r.GetArticles(page, key)
}