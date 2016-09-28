package main

import (
	"fmt"
	"github.com/AllenRay/GoRocketMQ/store"
)

import proto "github.com/golang/protobuf/proto"

func main() {
	mappedFile, err := store.CreateMappedFile("/home/leizhengyu/work/src/github.com/AllenRay/GoRocketMQ/start/0000000000", 10000000)

	if err != nil {
		fmt.Printf("The error is %s", err)
		return
	}
	//b := make([]byte, 10)

	mappedFile.AppendBytesMessage([]byte("this is byte.."))

	//mappedFile.AppendBytesMessage([]byte("another this is byte.."))

	wrotePosition := mappedFile.GetWrotePosition()

	fmt.Printf("Current position is %d\n", wrotePosition)

	maps := make(map[string]string)
	maps["aa"] = "aa"
	maps["bb"] = "bb"
	message := &store.Message{
		Topic:      "test",
		Flat:       int32(3),
		Properties: maps,
		Body:       "message body",
		QueueId:    int32(0),
	}

	mappedFile.AppendMessage(message)

	wrotePosition = mappedFile.GetWrotePosition()

	fmt.Printf("Current position is %d\n", wrotePosition)

	if mappedFile.IsAbleToFlush(4) {
		fmt.Println("flush to disk")
		mappedFile.Commit()
	} else {
		fmt.Println("cant flush to disk.")
	}

	data, _ := mappedFile.SelectMappedBuffer(15, 43)

	msg := &store.Message{}

	fmt.Println(data)

	proto.Unmarshal(data, msg)

	fmt.Println(msg.Body + ":" + msg.Topic)

	mappedFile.Release()

}
