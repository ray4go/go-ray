package log

import (
	"log"
	"os"
)

var (
	enableLog = false
	logger    *log.Logger

	Printf  func(format string, v ...any)
	Println func(v ...any)
)

var Panicf = log.Panicf

func init() {
	logger = log.New(os.Stderr, "[GO]", log.Lshortfile)
	Printf = logger.Printf
	Println = logger.Println
	Panicf = logger.Panicf
}

func Init(enable bool) {
	enableLog = enable
}

func Log(format string, args ...any) {
	if enableLog {
		logger.Printf(format, args...)
	}
}

func Debug(format string, args ...any) {
	Log(format, args...)
}
