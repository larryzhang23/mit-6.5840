package raft

import (
	"log"
	"runtime"
	"strconv"
	"strings"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func min(val1, val2 int) int {
	if val1 < val2 {
		return val1
	} else {
		return val2 
	}
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	// first line looks like "goroutine 123 [running]:"
	line := strings.Fields(string(b))[1]
	gid, err := strconv.ParseUint(line, 10, 64)
	if err != nil {
		panic(err)
	}
	return gid
}