package main

import (
	"fmt"
	"github.com/edsrzf/mmap"
	"os"
	//"syscall"
)

func main() {
	f, err := os.Create("/home/leizhengyu/work/src/github.com/AllenRay/GoRocketMQ/start/1111111123")

	defer f.Close()

	if err != nil {
		fmt.Printf("Open file %s occur error", "11111111")
		return
	}

	fmt.Println(f.Name())

	fileInfo, err := f.Stat()

	if err != nil {
		fmt.Printf("Get file %s info error %s", f.Name(), err)
		return
	}

	// _, err = f.Seek(1000, 0)
	// if err != nil {
	// 	fmt.Printf("seek file {} occur error", f.Name())
	// 	return
	// }

	// _, err = f.Write([]byte(" "))
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	err = f.Truncate(10000000)
	if err != nil {
		fmt.Printf("Truncate file %s error", f.Name())
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

	mmaped[2000] = 'x'

	mmaped.Flush()

	content := make([]byte, 2)

	f.ReadAt(content, 2000)

	fmt.Println(content)
}
