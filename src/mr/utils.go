package mr

import (
	"path/filepath"
	"strings"
	"log"
)

const DEBUG = false

func getDirFilename(file string) (string, string) {
	// Extract filename
	filename := filepath.Base(file) // "report.txt"
	// Extract Dir
	dir := filepath.Dir(file) // "/home/user/data"
	// Extract extension
	ext := filepath.Ext(filename) // ".txt"
	// Remove extension to get name only
	name := strings.TrimSuffix(filename, ext) // "report"
	
	return dir, name
}


func refactOutputFileName(filename string) string {
	parts := strings.Split(filename, "_")
	newFilename := strings.Join(parts[:3], "-")
	return newFilename
}


func DPrintf(format string, v ...any) {
	if DEBUG {
		log.Printf(format, v...)
	}
}