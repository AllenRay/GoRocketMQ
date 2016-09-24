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

type MappedFile struct {
	fileName                 string
	fileSize                 int32
	fileFromOffset           int32
	file                     *os.File
	totalMappedVirtualMemory int32
	totoalMappedFiles        int32
	wrotePosition            int32
	commitedPosition         int32
}

type AppendMessageCallback interface {
	DoAppend(fileFromOffset int32, file *os.File, maxBlank int32, msg *Message) bool
}

func DoAppend(fileFromOffset int32, file *os.File, maxBlank int32, msg *Message) bool {
	return false
}

func CreateMappedFile(fileName string, fileSize int32) (*MappedFile, error) {
	file, err := os.Create(fileName)
	if err != nil {
		log.Panicf("thie file %s created error.", fileName)
		return nil, err
	}
	dir := filepath.Dir(fileName)

	fmt.Printf("file dir is %s ", dir)

	fileFromOffset, err := strconv.Atoi(filepath.Base(fileName))
	if err != nil {
		log.Panicf("The file name %s cant convert to int.", file.Name())
		return nil, err
	}

	//mmap
	//mmap, err := mmap.Map(file, mmap.RDWR, 0)

	return &MappedFile{
		fileName:       fileName,
		fileSize:       fileSize,
		fileFromOffset: int32(fileFromOffset),
		file:           file,
		totalMappedVirtualMemory: 0,
		totoalMappedFiles:        0,
		wrotePosition:            0,
		commitedPosition:         0,
	}, nil

}

func (mappedFile *MappedFile) AppendMessage(msg *Message) (bool, error) {

	currentPosition := mappedFile.wrotePosition

	if currentPosition < mappedFile.fileSize {

	}

	return false, nil
}

func (mappedFile *MappedFile) AppendBytesMessage(data []byte) (bool, error) {
	currentPosition := mappedFile.wrotePosition

	if currentPosition < mappedFile.fileSize {

		size, err := mappedFile.file.WriteAt(data, int64(currentPosition+1))

		if err != nil {
			log.Panicf("write data to file %s occur error", mappedFile.file.Name())
			return false, err
		}

		atomic.AddInt32(&mappedFile.wrotePosition, int32(len(data)))

		return true, nil
	}

	return false, nil
}

func (mappedFile *MappedFile) GetWrotePosition() int32 {
	return mappedFile.wrotePosition
}

func (mappedFile *MappedFile) GetFileFromOfferset() int32 {
	return mappedFile.fileFromOffset
}
