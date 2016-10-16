package debug

import (
	"log"
	"os"
)

// Verbosity levels.
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

// SetVerbosity sets the verbosity level.
func SetVerbosity(level int) {
	verbosity = level
}

// Debug logs if verbosity >= DEBUG.
func Debug(v ...interface{}) {
	if verbosity >= DEBUG {
		logger.Print(v...)
	}
}

// Debugf logs if verbosity >= DEBUG.
func Debugf(format string, v ...interface{}) {
	if verbosity >= DEBUG {
		logger.Printf(format, v...)
	}
}

// Debugln logs if verbosity >= DEBUG.
func Debugln(v ...interface{}) {
	if verbosity >= DEBUG {
		logger.Println(v...)
	}
}

// Trace logs if verbosity >= DEBUG.
func Trace(v ...interface{}) {
	if verbosity >= TRACE {
		logger.Print(v...)
	}
}

// Tracef logs if verbosity >= DEBUG.
func Tracef(format string, v ...interface{}) {
	if verbosity >= TRACE {
		logger.Printf(format, v...)
	}
}

// Traceln logs if verbosity >= DEBUG.
func Traceln(v ...interface{}) {
	if verbosity >= TRACE {
		logger.Println(v...)
	}
}
