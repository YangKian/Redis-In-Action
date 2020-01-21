package repository

import (
	"database/sql"
	"redisInAction/test/models"
)

type UserRepo struct {
	db *sql.DB
}

func NewUserRepo(db *sql.DB) *UserRepo {
	return &UserRepo{db: db}
}

func (r *UserRepo) FindByID(ID int) (*models.User, error) {
	return &models.User{}, nil
}

func (r *UserRepo) Save(user *models.User) error {
	return nil
}
