package main

import (
	"fmt"
	"os"
	"raft/internal/raft"
)

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: raft <port>")
		os.Exit(2)
	}
	if err := raft.Init(args[1]); err != nil {
		fmt.Printf("Something went wrong %v", err)
	}
}
