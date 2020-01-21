package models

type User struct {
	Name string
}

type UserRepository interface {
	FindByID(int) (*User, error)
	Save(*User) error
}
