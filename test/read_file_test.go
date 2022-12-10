package test

import (
	"mapreduce/util"
	"os"
	"testing"
)

func TestProcess(t *testing.T) {
	// Create doc
	file, err := os.Create("test.txt")
	if err != nil {
		t.Errorf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Write file
	content := []byte("Hello world!\nThis is a test.\n")
	_, err = file.Write(content)
	if err != nil {
		t.Errorf("Failed to write to file: %v", err)
	}

	// execute Process funtion
	err = util.FileProcess(0, file)
	if err != nil {
		t.Errorf("Process failed: %v", err)
	}
}
