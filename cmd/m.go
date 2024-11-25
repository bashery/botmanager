package main

import (
	"encoding/binary"
	"fmt"
)

func mainc() {
	// Example uint8 value
	var num uint8 = 122

	// Convert uint8 to []byte
	byteSlice := []byte{num}

	// Print the result
	fmt.Println(byteSlice) // Output: [42]

	var num2 uint64 = 122

	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, num2) //PutUnit64(bs, num2)
	fmt.Println(bs)

}
