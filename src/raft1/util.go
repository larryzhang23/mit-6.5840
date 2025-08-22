package raft

import "log"

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