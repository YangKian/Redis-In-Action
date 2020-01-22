package main

import (
	"fmt"
	"math/rand"
)

func main() {
	for i := 0; i < 20; i++ {
		res := rand.Intn(5)
		fmt.Println(res)
	}

}
