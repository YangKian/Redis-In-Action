package main

import (
	"database/sql"
	"redisInAction/test/controller"
	"redisInAction/test/repository"
)

func main() {
	db, _ := sql.Open("mysql", "sql.db")
	userRepo := repository.NewUserRepo(db)

	h := controller.NewBaseHandler(userRepo)
}
