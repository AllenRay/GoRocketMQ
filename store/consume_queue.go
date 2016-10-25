package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

type ConsumeQueue struct {
	mappedFileQueue *MappedFileQueue
	topic           string
	queueId         int32
	byteBuffer      bytes.Buffer //correct?
	storePath       string
	mappedFileSize  int32
	maxPhysicOffset int64
	minLogicOffset  int64
}

var maxPhysicOffset int64 = -1

const (
	CQStoreUnitSize = 20
)

func (c *ConsumeQueue) PutMessagePositionInfoWrapper(offset, tagsCode, storeTimeStamp, logicOffset int64, size int32) {
	var maxRetries int32 = 5

	for i := 0; i < maxRetries; i++ {
		result := putMessagePositionInfo(offset, tagsCode, logicOffset, size)

		if result {
			return
		} else {
			time.Sleep(time.Second * 1)
		}
	}

}

func (c *ConsumeQueue) putMessagePositionInfo(offset, tagsCode, cqOffset int64, size int32) bool {
	if offset <= c.maxPhysicOffset {
		return true
	}

	c.byteBuffer.Write(int64ToByte(offset))
	c.byteBuffer.Write(int32ToByte(size))
	c.byteBuffer.Write(int64ToByte(tagsCode))

	expectLogicOffset := cqOffset * 20

	mappedFile, err := c.mappedFileQueue.GetLastMappedFile(expectLogicOffset)
	if err != nil {
		fmt.Println("get Mapped file occur error.")
		return false
	}

	tempBytes := make([]byte, 20)
	//clear byte buffer
	c.byteBuffer.Read(tempBytes)

	ok, err := mappedFile.AppendBytesMessage(tempBytes)

	if err != nil {
		fmt.Println("append message failed")
		return false
	}

	return true

}

func (c *ConsumeQueue) CorrectMinOffset(phyMinOffset int64) {
	mappedFile := c.mappedFileQueue.getFirstMappedFiles()

	if mappedFile != nil {
		data, err := mappedFile.SelectMappedBufferByPos(0)
		if err == nil && mappedFile != nil {
			buf := bytes.NewBuffer(byteArray)
			var offsetBy int64
			for i := 0; i < len(data); i += CQStoreUnitSize {
				//read fist eight bytes when each loop
				b := data[i : i+8]
				buf.Write(b)
				binary.Read(buf, binary.BigEndian, offsetBy)

				if offsetBy >= phyMinOffset {
					c.minLogicOffset = mappedFile.GetFileFromOfferset() + i
					break
				}

			}

		}
	}

}

func (c *ConsumeQueue) GetMinOffsetInQueue() {
	return c.minLogicOffset / CQStoreUnitSize
}

func (c *ConsumeQueue) GetMinOffset() {
	return c.minLogicOffset
}

func (c *ConsumeQueue) GetIndexBuffer(startIndex int64) []byte {
	mappedFileSize := c.mappedFileSize
	offset := startIndex * CQStoreUnitSize
	if offset >= c.GetMinOffset() {
		mappedFile, err := c.mappedFileQueue.FindMappedFileByOffsetReturnNil(offset)

		if err == nil && mappedFile != nil {
			data, err := mappedFile.SelectMappedBufferByPos(int(offset % mappedFileSize))

			if err == nil && data != nil {
				return data
			}
		}
	}

	return nil
}

func (c *ConsumeQueue) RollNextFile(index int64) int64 {
	mappedFileSzie := c.mappedFileSize
	totalUnitsInFile := mappedFileSzie / CQStoreUnitSize

	return (index + int64(totalUnitsInFile) - index%int64(totalUnitsInFile))
}

func int32ToByte(v int32) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, v)
	return bytesBuffer.Bytes()
}

func int64ToByte(v int64) []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, v)
	return buf.Bytes()
}
