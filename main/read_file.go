// Ref from https://gist.github.com/ohmpatel1997/3b5b164d11053ca82cbe901e35b6c0d3#file-log-extraction-go
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

var BLOCK_SIZE = 4 * 1024

func Process(fileCount int, f *os.File) error {

	linesPool := sync.Pool{
		New: func() interface{} {
			lines := make([]byte, BLOCK_SIZE)
			return lines
		},
	}

	r := bufio.NewReader(f)

	var wg sync.WaitGroup

	blockCount := 0
	ch := make(chan int, 1)
	ch <- blockCount
	for {
		buf := linesPool.Get().([]byte)

		n, err := r.Read(buf)
		buf = buf[:n]

		if n == 0 {
			if err != nil {
				fmt.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
			return err
		}

		nextUntillNewline, err := r.ReadBytes('\n')

		if err != io.EOF {
			buf = append(buf, nextUntillNewline...)
		}

		wg.Add(1)
		go func(blockCount int) {
			// TODO: send to worker
			fmt.Printf("%d, %d\n", fileCount, blockCount)
			fmt.Printf("%s\n\n", string(buf))
			fmt.Printf("=======================================================\n")
			wg.Done()
		}(blockCount)
		blockCount++
	}

	wg.Wait()
	return nil
}
