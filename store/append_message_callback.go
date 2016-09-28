package store

import proto "github.com/golang/protobuf/proto"

import (
	"github.com/edsrzf/mmap"
	"log"
)

func DoAppend(currentPosition int32, mappedfile mmap.MMap, maxBlank int32, msg *Message) (AppendMessageResut, error) {

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Panicf("use proto masrshal msg occur err %s", err)
		return AppendMessageResut{
			appendMessageStatus: UNKNOWN_ERROR,
		}, err
	}

	if len(data) > int(maxBlank) {
		return AppendMessageResut{
			appendMessageStatus: END_OF_FILE,
		}, nil
	}

	for i := 0; i < len(data); i++ {
		mappedfile[int(currentPosition)+i] = data[i]
	}

	return AppendMessageResut{
		appendMessageStatus: PUT_OK,
		wroteBytes:          int32(len(data)),
	}, nil
}
