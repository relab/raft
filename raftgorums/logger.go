package raftgorums

import "log"

// LogLevel sets the level of log verbosity.
// TODO Remove or at least make configuration.
type LogLevel int

// Log verbosity levels.
const (
	OFF LogLevel = iota
	INFO
	DEBUG
	TRACE
)

const logLevel = DEBUG

// Logger wraps a stdlib Logger with a server id.
type Logger struct {
	id uint64
	*log.Logger
}

func (l *Logger) log(message string) {
	l.Printf("%d: %s", l.id, message)
}

func (l *Logger) to(to uint64, message string) {
	l.Printf("%d -> %d: %s", l.id, to, message)
}

func (l Logger) from(from uint64, message string) {
	l.Printf("%d <- %d: %s", l.id, from, message)
}
