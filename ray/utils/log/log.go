package log

import "log"

var (
	enableLog bool = false
)

func Init(enable bool) {
	enableLog = enable
	if enable {
		log.SetFlags(log.Lshortfile)
	}
}

func Log(format string, args ...any) {
	if enableLog {
		log.Printf(format, args...)
	}
}

func Debug(format string, args ...any) {
	Log("[DEBUG]"+format, args...)
}

var Panicf = log.Panicf
