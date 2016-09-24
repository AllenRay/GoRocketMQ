package main

import (
	"fmt"
	"github.com/edsrzf/mmap"
	"os"
	//"syscall"
)

func main() {
	f, err := os.OpenFile("/data/kafka-logs/111111", os.O_RDWR, 0644)

	defer f.Close()

	if err != nil {
		fmt.Printf("Open file %s occur error", "11111111")
		return
	}
	fmt.Println(f.Name())

	fileInfo, err := f.Stat()

	if err != nil {
		fmt.Printf("Get file {} info error {}", f.Name(), err)
		return
	}

	fileSize := fileInfo.Size()

	fmt.Println(fileSize)

	if fileSize <= 0 {
		fileSize = 10000000
	}

	// mmapedData, err := syscall.Mmap(int(uintprt), 0, int(fileSize), syscall.PROT_WRITE, syscall.MAP_SHARED)
	// if err != nil {
	// 	fmt.Printf("mmap file %s occur error %s", f.Name(), err)
	// 	return
	// }

	//mmapedData = append(mmapedData, 'x')
	//fmt.Println(mmapedData)

	//mmapedData[0] = 'a'

	//fmt.Println(mmap.RDWR)

	mmaped, err := mmap.MapRegion(f, int(fileSize), mmap.RDWR, 0, 0)
	if err != nil {
		fmt.Println(err)
		fmt.Printf("mmap file %s occur error", f.Name())
		return
	}

	defer mmaped.Unmap()

	mmaped[0] = 'x'

	mmaped.Flush()
}
