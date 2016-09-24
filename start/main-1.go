package main

import (
	"fmt"
	"github.com/edsrzf/mmap"
	"os"
)

func main() {
	f, err := os.OpenFile("/data/kafka-logs/111111", os.O_RDWR, 0644)

	defer f.Close()

	if err != nil {
		fmt.Printf("Open file %s occur error", "11111111")
		return
	}
	fmt.Println(f.Name())

	fmt.Println(mmap.RDWR)

	mmaped, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		fmt.Println(err)
		fmt.Printf("mmap file %s occur error", f.Name())
		return
	}

	defer mmaped.Unmap()

	mmaped[1] = 'x'

	mmaped.Flush()
}
