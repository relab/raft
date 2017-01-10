package raft

import "log"

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
