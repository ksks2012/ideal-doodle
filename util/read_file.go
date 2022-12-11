// Ref from https://gist.github.com/ohmpatel1997/3b5b164d11053ca82cbe901e35b6c0d3#file-log-extraction-go
package util

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

const BLOCK_SIZE = 4 * 1024

func FileProcess(fileCount int, fileName string) error {

	file, err := os.Open(fileName)

	if err != nil {
		fmt.Println("cannot able to read the file", err)
		return err
	}

	defer file.Close() //close after checking err

	linesPool := sync.Pool{
		New: func() interface{} {
			lines := make([]byte, BLOCK_SIZE)
			return lines
		},
	}

	r := bufio.NewReader(file)

	var wg sync.WaitGroup

	blockCount := 0
	for {
		buf := linesPool.Get().([]byte)
		defer linesPool.Put(buf)

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
