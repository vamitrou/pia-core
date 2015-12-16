package pialog

import (
	"log"
	"os"
)

const (
	TRACE = iota
	INFO
	WARN
	ERROR
)

var tracelogger log.Logger
var infologger log.Logger
var warnlogger log.Logger
var errorlogger log.Logger

var loggermap = make(map[int]*log.Logger)

var levelStrings = map[int]string{
	TRACE: "TRACE: ",
	INFO:  "INFO: ",
	WARN:  "WARN: ",
	ERROR: "ERROR: ",
}

func InitializeLogging() {
	for i := 0; i < len(levelStrings); i++ {
		loggermap[i] = log.New(os.Stdout, levelStrings[i], log.Ldate|log.Ltime)
	}
}

func Log(level int, msg ...interface{}) {
	loggermap[level].Println(msg...)
}

func Trace(msg ...interface{}) {
	Log(TRACE, msg...)
}

func Info(msg ...interface{}) {
	Log(INFO, msg...)
}

func Warn(msg ...interface{}) {
	Log(WARN, msg...)
}

func Error(msg ...interface{}) {
	Log(ERROR, msg...)
}
