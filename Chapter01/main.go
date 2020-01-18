package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"redisInAction/pkg/common"
	"redisInAction/pkg/redisConn"
	"strconv"
	"strings"
	"time"
)

func init() {
	common.DB = 0
	common.Addr = "192.168.1.6:6379"
	common.Password = ""
}

func ArticleVote(conn *redis.Client, article, user string) {
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	if conn.ZScore("time", article).Val() < float64(cutoff) {
		return
	}

	articleId := strings.Split(article, ":")[1]
	if conn.SAdd("voted:"+articleId, user).Val() != 0 {
		conn.ZIncrBy("score", common.VoteScore, article)
		conn.HIncrBy(article, "votes", 1)
	}
}

func PostArticle(conn *redis.Client, user, title, link string) string {
	articleId := strconv.Itoa(int(conn.Incr("article:").Val()))

	voted := "voted:" + articleId
	conn.SAdd(voted, user)
	conn.Expire(voted, common.OneWeekInSeconds*time.Second)

	now := time.Now().Unix()
	article := "article:" + articleId
	conn.HMSet(article, map[string]interface{}{
		"title":  title,
		"link":   link,
		"poster": user,
		"time":   now,
		"votes":  1,
	})

	conn.ZAdd("score:", redis.Z{Score: float64(now + common.VoteScore), Member: article})
	conn.ZAdd("time", redis.Z{Score: float64(now), Member: article})
	return articleId
}

func GetArticles(conn *redis.Client, page int64, order string) []map[string]string {
	if order == "" {
		order = "score:"
	}
	start := (page - 1) * common.ArticlesPerPage
	end := start + common.ArticlesPerPage - 1

	ids := conn.ZRevRange(order, start, end).Val()
	articles := []map[string]string{}
	for _, id := range ids {
		articleData := conn.HGetAll(id).Val()
		articleData["id"] = id
		articles = append(articles, articleData)
	}
	return articles
}

func AddRemoveGroups(conn *redis.Client, articleId string, toAdd, toRemove []string) {
	article := "article:" + articleId
	for _, group := range toAdd {
		conn.SAdd("group:"+group, article)
	}
	for _, group := range toRemove {
		conn.SRem("group:"+group, article)
	}
}

func GetGroupArticles(conn *redis.Client, group, order string, page int64) []map[string]string {
	if order == "" {
		order = "score:"
	}
	key := order + group
	if conn.Exists(key).Val() == 0 {
		res := conn.ZInterStore(key, redis.ZStore{Aggregate: "MAX"}, "group:"+group, order).Val()
		if res <= 0 {
			log.Println("ZInterStore return 0")
		}
	}
	conn.Expire(key, 60*time.Second)
	return GetArticles(conn, page, key)
}

func main() {
	conn := (*redis.Client)(redisConn.ConnectRedis())

	articleId := PostArticle(conn, "username", "A title", "http://www.google.com")
	fmt.Println("We posted a new article with id: ", articleId)

	r := conn.HGetAll("article:" + articleId)
	fmt.Println("\nIts HASH looks like: ", r)

	ArticleVote(conn, "article:"+articleId, "other_user")
	v, _ := conn.HGet("article:"+articleId, "votes").Int()
	if v <= 0 {
		log.Fatal("\nThe voted num should be greatter than 0\n")
	}
	fmt.Println("\nWe voted for the article, it now has votes: ", v)

	fmt.Println("\nThe currently highest-scoring articles are: ")
	articles := GetArticles(conn, 1, "")
	if len(articles) <= 0 {
		log.Fatal("\nThe num of articles should be greatter than 0\n")
	}
	for k, v := range articles {
		fmt.Println(k, v)
	}

	AddRemoveGroups(conn, articleId, []string{"new-group"}, []string{})
	articles = GetGroupArticles(conn, "new-group", "score:", 1)
	if len(articles) <= 0 {
		log.Fatal("\nThe num of articles should be greatter than 0\n")
	}
	fmt.Println("\nWe added the article to a new group, other articles include: ")
	for k, v := range articles {
		fmt.Println(k, v)
	}

	delKeys := []string{"time:*", "voted:*", "score:*", "article:*", "group:*"}
	var toDel []string
	for _, v := range delKeys {
		toDel = append(toDel, conn.Keys(v).Val()...)
	}

	if len(toDel) != 0 {
		conn.Del(toDel...)
	}
}
