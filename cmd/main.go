package main

import (
	"store"
)

func main() {
	eng := store.NewDatabase("mydb")
	for coll, ok := range eng.Collections {
		println(coll, ok)
	}

	eng.Close()
}
