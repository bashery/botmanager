package main

import (
	"fmt"
	"store"
)

func main() {
	db, err := store.NewDatabase("mydb")
	if err != nil {
		fmt.Println("create db err", err)
	}

	err = db.Insert("users", "hello")

	if err != nil {
		fmt.Println("at insert:", err)
	}

	err = db.Close()
	if err != nil {
		fmt.Println("at close:", err)
	}
}
