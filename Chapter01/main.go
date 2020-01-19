package main

import (
	"fmt"
	"log"
	"redisInAction/Chapter01/pkg/redisConn"
)

func main() {
	conn := redisConn.ConnectRedis()

	articleId := conn.PostArticle("username", "A title", "http://www.google.com")
	fmt.Println("We posted a new article with id: ", articleId)

	r := conn.HGetAll("article:" + articleId)
	fmt.Println("\nIts HASH looks like: ", r)

	conn.ArticleVote("article:"+articleId, "other_user")
	v, _ := conn.HGet("article:"+articleId, "votes").Int()
	if v <= 0 {
		log.Fatal("\nThe voted num should be greatter than 0\n")
	}
	fmt.Println("\nWe voted for the article, it now has votes: ", v)

	fmt.Println("\nThe currently highest-scoring articles are: ")
	articles := conn.GetArticles(1, "")
	if len(articles) <= 0 {
		log.Fatal("\nThe num of articles should be greatter than 0\n")
	}
	for k, v := range articles {
		fmt.Println(k, v)
	}

	conn.AddRemoveGroups(articleId, []string{"new-group"}, []string{})
	articles = conn.GetGroupArticles("new-group", "score:", 1)
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
