package store

import "sync"
import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	//"log"
)

const (
	DELETE_FILA_BATCH_MAX = 10
)

var lock sync.Mutex

type MappedFileQueue struct {
	StorePath      string
	MappedFileSize int32
	MapedFiles     *list.List
}

/*func init() {
	log.New(out, prefix, flag)
}*/

func putRequestAndReturnMappedFile(nextFilePath string, fileSize int) (*MappedFile, error) {
	mappedFile, err := CreateMappedFile(nextFilePath, int32(fileSize))

	if err != nil {
		fmt.Printf("create mapped file occur error %s", err)
		return nil, err
	}

	return mappedFile, nil
}

func (mappedFileQueue *MappedFileQueue) GetLastMappedFileOffsetIsZero() (*MappedFile, error) {
	return mappedFileQueue.GetLastMappedFile(0)
}

func (mappedFileQueue *MappedFileQueue) GetLastMappedFile(startOffset int64) (*MappedFile, error) {
	lock.Lock()

	defer lock.Unlock()

	var createOffset int32 = -1

	if mappedFileQueue.MapedFiles.Len() <= 0 {
		createOffset = int32(startOffset - (startOffset % int64(mappedFileQueue.MappedFileSize)))
	} else {
		v := mappedFileQueue.MapedFiles.Back().Value

		mappedFileLast, ok := v.(MappedFile)

		if ok {
			if mappedFileLast.IsFull() {
				createOffset = mappedFileLast.fileFromOffset + mappedFileQueue.MappedFileSize
			}
		} else {
			return &mappedFileLast, nil
		}
	}

	if createOffset != -1 {
		f := strconv.Itoa(int(createOffset))
		fileName := LeafPad(f, strconv.Itoa(0), 20-len(f))
		//fileName, err := fmt.Sprintf("%20s", createOffset)
		fmt.Printf("file name is : %s", fileName)
		nextFilePath := mappedFileQueue.StorePath + "/" + fileName
		mappedFile, err := putRequestAndReturnMappedFile(nextFilePath, int(mappedFileQueue.MappedFileSize))
		if err != nil {
			fmt.Printf("create mapped file error %s", err)
			return nil, err
		}
		mappedFileQueue.MapedFiles.PushBack(mappedFile)

		return mappedFile, nil
	}

	return nil, nil

}

func (mappedFileQueue *MappedFileQueue) GetMappedFileByTime(timeStamp int64) (*MappedFile, error) {

	lock.Lock()
	defer lock.Unlock()

	for e := mappedFileQueue.MapedFiles.Front(); e != nil; e.Next() {
		v := e.Value
		mappedFile, ok := v.(MappedFile)
		if !ok {
			fmt.Printf("the value %s is not mapped file.", v)
			return nil, errors.New("Type is not correct")
		}

		fileInfo, _ := mappedFile.file.Stat()

		modTime := fileInfo.ModTime().UnixNano()

		if modTime > timeStamp {
			return &mappedFile, nil
		}

	}

	return nil, nil
}

func (mappedFileQueue *MappedFileQueue) TruncateDirtyFiles(offset int32) {
	if mappedFileQueue.MapedFiles.Len() <= 0 {
		fmt.Println("no any mapped file,return.")
	} else {
		for e := mappedFileQueue.MapedFiles.Front(); e != nil; e.Next() {
			mappedFile := e.Value.(MappedFile)

			fileTailOffset := mappedFile.GetFileFromOfferset() + mappedFileQueue.MappedFileSize

			if fileTailOffset > offset && offset > mappedFile.GetFileFromOfferset() {
				mappedFile.SetCommitPosition(int32(offset % mappedFileQueue.MappedFileSize))
				mappedFile.SetWrotePosition(int32(offset % mappedFileQueue.MappedFileSize))
			} else {
				mappedFileQueue.MapedFiles.Remove(e)
				mappedFile.Destory()
			}
		}
	}
}

func (mappedFileQueue *MappedFileQueue) GetMaxOffset() int64 {
	lock.Lock()

	defer lock.Unlock()

	if mappedFileQueue.MapedFiles.Len() > 0 {
		value := mappedFileQueue.MapedFiles.Back().Value

		mappedFile := value.(MappedFile)

		return int64(mappedFile.GetFileFromOfferset() + mappedFile.GetWrotePosition())
	}

	return 0
}

func (mappedFileQueue *MappedFileQueue) DeleteLastMappedFile() {
	if mappedFileQueue.MapedFiles.Len() > 0 {

		value := mappedFileQueue.MapedFiles.Back().Value

		mappefile := value.(MappedFile)

		mappedFileQueue.MapedFiles.Remove(mappedFileQueue.MapedFiles.Back())

		mappefile.Destory()

	}
}

func (mappedFileQueue *MappedFileQueue) DeleteExpiredFileByTime(expiredTIme int64, deleteFilesInterval int32, cleanImmediately bool) int {
	if mappedFileQueue.MapedFiles.Len() > 0 {

		var index int = 0
		var deleteCount int = 0

		for e := mappedFileQueue.MapedFiles.Front(); e != nil; e.Next() {
			mappedFile := e.Value.(MappedFile)

			if index < mappedFileQueue.MapedFiles.Len()-1 {
				fileInfo, _ := os.Stat(mappedFile.file.Name())
				liveMaxTimestamp := fileInfo.ModTime().UnixNano() + expiredTIme
				if time.Now().UnixNano() > liveMaxTimestamp {
					mappedFile.Destory() //remove mapped file
					mappedFileQueue.MapedFiles.Remove(e)
					deleteCount++
				}

			}
			index++
		}

		return deleteCount

	}

	return 0

}

func (mappefFileQueue *MappedFileQueue) getFirstMappedFiles() *MappedFile {
	if mappefFileQueue.MapedFiles.Len() <= 0 {
		return nil
	}

	value := mappefFileQueue.MapedFiles.Front().Value

	mappedFile, ok := value.(MappedFile)

	if ok {
		return &mappedFile
	}

	return nil
}

func (mappedFileQueue *MappedFileQueue) FindMappedFileByOffset(offset int64, returnFirstNotFound bool) (*MappedFile, error) {
	lock.Lock()

	defer lock.Unlock()

	mappedFile := mappedFileQueue.getFirstMappedFiles()

	if mappedFile != nil {
		index := int(int32(offset)/mappedFileQueue.MappedFileSize) - int(mappedFile.GetFileFromOfferset()/mappedFileQueue.MappedFileSize)

		if index <= 0 || index >= mappedFileQueue.MapedFiles.Len() {
			fmt.Printf("The index %d is wrong number", index)
			return nil, fmt.Errorf("The index %d is wrong number", index)
		}

		if index == 0 {
			return mappedFile, nil
		} else if index == mappedFileQueue.MapedFiles.Len()-1 {
			value := mappedFileQueue.MapedFiles.Back().Value

			mFile, ok := value.(MappedFile)

			if ok {
				return &mFile, nil
			}
		} else {
			var count int = 0
			for e := mappedFileQueue.MapedFiles.Front(); e != nil; e.Next() {
				if index == count {
					value := mappedFileQueue.MapedFiles.Back().Value

					mFile, ok := value.(MappedFile)

					if ok {
						return &mFile, nil
					}
				}

				count++
			}

			if returnFirstNotFound {
				return mappedFile, nil
			}
		}

	}

	return nil, errors.New("No any mapped file")

}

//return nil when not found mapped file by offset
func (mappedFileQueue *MappedFileQueue) FindMappedFileByOffsetReturnNil(offset int64) (*MappedFile, error) {
	return mappedFileQueue.FindMappedFileByOffset(offset, false)
}

func LeafPad(s, padString string, padLen int) string {
	return strings.Repeat(padString, padLen) + s
}
