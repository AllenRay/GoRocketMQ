package store

import (
	"fmt"
	"github.com/edsrzf/mmap"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
)

//import proto "github.com/golang/protobuf/proto"
//import golangmmap "golang.org/x/exp/mmap"

const (
	//PAGE CACHE SIZE
	OS_PAGE_SIZE = 1024 * 4

	//FLUSH page
	FLUSH_LEAST_PAGE = 4
)

type MappedFile struct {
	fileName                 string
	fileSize                 int32
	fileFromOffset           int32
	file                     *os.File
	mappedFile               mmap.MMap
	totalMappedVirtualMemory int32
	totoalMappedFiles        int32
	wrotePosition            int32
	commitedPosition         int32
}

type AppendMessageCallback interface {
	DoAppend(currentPosition int32, mappedfile mmap.MMap, maxBlank int32, msg *Message) (AppendMessageResut, error)
}

func CreateMappedFile(fileName string, fileSize int32) (*MappedFile, error) {
	file, err := os.Create(fileName)
	if err != nil {
		log.Panicf("thie file %s created error.", fileName)
		return nil, err
	}

	fileFromOffset, err := strconv.Atoi(filepath.Base(fileName))
	if err != nil {
		log.Panicf("The file name %s cant convert to int.", file.Name())
		return nil, err
	}

	file.Truncate(int64(fileSize))

	mmap, err := mmap.MapRegion(file, int(fileSize), mmap.RDWR, 0, 0)
	if err != nil {
		log.Panicf("Mmap file %s occur error,so abort", file.Name())
		os.Exit(1)
	}

	return &MappedFile{
		fileName:                 fileName,
		fileSize:                 fileSize,
		fileFromOffset:           int32(fileFromOffset),
		file:                     file,
		mappedFile:               mmap,
		totalMappedVirtualMemory: 0,
		totoalMappedFiles:        0,
		wrotePosition:            0,
		commitedPosition:         0,
	}, nil

}

func (mappedFile *MappedFile) AppendMessage(msg *Message) (AppendMessageResut, error) {

	currentPosition := mappedFile.wrotePosition

	if currentPosition < mappedFile.fileSize {

		maxBlank := mappedFile.fileSize - currentPosition

		result, err := DoAppend(currentPosition, mappedFile.mappedFile, maxBlank, msg)
		if err != nil {
			return AppendMessageResut{
				appendMessageStatus: UNKNOWN_ERROR,
			}, err
		}

		wroteBytes := result.wroteBytes

		fmt.Printf("append message successful,wrote bytes is %d\n", wroteBytes)

		atomic.AddInt32(&mappedFile.wrotePosition, int32(wroteBytes)+1)

		return result, err

	}

	return AppendMessageResut{
		appendMessageStatus: UNKNOWN_ERROR,
	}, nil
}

func (mappedFile *MappedFile) AppendBytesMessage(data []byte) (bool, error) {
	currentPosition := mappedFile.wrotePosition

	if currentPosition+int32(len(data)) < mappedFile.fileSize {

		for i := 0; i < len(data); i++ {
			mappedFile.mappedFile[int(currentPosition)+i] = data[i]
		}

		//mappedFile.mappedFile.Flush()

		atomic.AddInt32(&mappedFile.wrotePosition, int32(len(data))+1)

		return true, nil
	}

	return false, nil
}

func (mappedFile *MappedFile) GetWrotePosition() int32 {
	return mappedFile.wrotePosition
}

func (mappedFile *MappedFile) SetWrotePosition(wrotePostion int32) {
	atomic.AddInt32(&mappedFile.wrotePosition, wrotePostion)
}

func (mappedFile *MappedFile) SetCommitPosition(commitPosition int32) {
	atomic.AddInt32(&mappedFile.commitedPosition, commitPosition)
}

func (mappedFile *MappedFile) GetFileFromOfferset() int32 {
	return mappedFile.fileFromOffset
}

//relase resource
func (mappedFile *MappedFile) Release() {

	mappedFile.mappedFile.Unmap()

	mappedFile.file.Close()

}

//flush to disk.
func (mappedFile *MappedFile) Commit() {
	value := mappedFile.wrotePosition

	mappedFile.mappedFile.Flush()

	atomic.AddInt32(&mappedFile.commitedPosition, value)
}

//is able to flush
func (mappedFile *MappedFile) IsAbleToFlush(flushAtLeastPage int) bool {
	if mappedFile.IsFull() {
		fmt.Println("the file is full")
		return true
	}

	wrotePosition := mappedFile.wrotePosition
	commitPosition := mappedFile.commitedPosition

	if flushAtLeastPage > 0 {

		return ((wrotePosition/int32(OS_PAGE_SIZE))-(commitPosition/int32(OS_PAGE_SIZE)) > int32(flushAtLeastPage))
	}

	return wrotePosition > commitPosition

}

//is full.
func (mappedFile *MappedFile) IsFull() bool {
	wrotePosition := mappedFile.wrotePosition

	fileSize := mappedFile.fileSize

	if wrotePosition == fileSize {
		return true
	}

	return false
}

func (mappedFile *MappedFile) SelectMappedBuffer(pos, size int) ([]byte, error) {
	if int32(pos+size) <= mappedFile.wrotePosition {

		data := make([]byte, size)
		copy(data, mappedFile.mappedFile[pos:])
		return data, nil
	}

	return nil, nil
}

func (mappedFile *MappedFile) SelectMappedBufferByPos(pos int) ([]byte, error) {
	if int32(pos) < mappedFile.wrotePosition && pos >= 0 {
		size := mappedFile.wrotePosition - int32(pos)
		data := make([]byte, int(size))
		copy(data, mappedFile.mappedFile[pos:])
		return data, nil
	}

	return nil, nil

}

func (mappedFile *MappedFile) Destory() {
	//first invoke release
	mappedFile.Release()

	os.Remove(mappedFile.file.Name())
}
