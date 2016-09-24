package main

import (
	"fmt"
	"github.com/GoRocketMQ/store"
)

func main() {
	mappedFile, err := store.CreateMappedFile("/Users/leizhenyu/store/consumequeue/000000000", 1000000000)

	if err != nil {
		fmt.Printf("The error is %s", err)
		return
	}
	//b := make([]byte, 10)

	mappedFile.AppendBytesMessage([]byte("this is byte.."))

	//mappedFile.AppendBytesMessage([]byte("another this is byte.."))

	wrotePosition := mappedFile.GetWrotePosition()

	fmt.Printf("Current position is %d", wrotePosition)
}
