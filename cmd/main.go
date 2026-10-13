package main

import (
	"fmt"
	"os"
	"raft/internal/raft"
)

func main() {
	args := os.Args
	if err := raft.Init(args[1]); err != nil {
		fmt.Printf("Something went wrong %v", err)
	}
}
