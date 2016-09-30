package debug

import (
	"log"
	"os"
)

const (
	OFF = iota
	DEBUG
	TRACE
)

var verbosity int
var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "[raft] ", log.Lmicroseconds)
}

func SetVerbosity(level int) {
	verbosity = level
}

func Debug(v ...interface{}) {
	if verbosity >= DEBUG {
		logger.Print(v...)
	}
}

func Debugf(format string, v ...interface{}) {
	if verbosity >= DEBUG {
		logger.Printf(format, v...)
	}
}

func Debugln(v ...interface{}) {
	if verbosity >= DEBUG {
		logger.Println(v...)
	}
}

func Trace(v ...interface{}) {
	if verbosity >= TRACE {
		logger.Print(v...)
	}
}

func Tracef(format string, v ...interface{}) {
	if verbosity >= TRACE {
		logger.Printf(format, v...)
	}
}

func Traceln(v ...interface{}) {
	if verbosity >= TRACE {
		logger.Println(v...)
	}
}
