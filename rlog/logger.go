package rlog

import "log"

type Logger interface {
	Log(message string)
	LogFrom(from uint32, message string)
	LogTo(to uint32, message string)
}

func NewStdLibLogger(id uint32, logger *log.Logger) Logger {
	return &stdLibLogger{
		id: id,
		l:  logger,
	}
}

type stdLibLogger struct {
	id uint32
	l  *log.Logger
}

func (s *stdLibLogger) Log(message string) {
	s.l.Printf("%d: %s", s.id, message)
}

func (s *stdLibLogger) LogFrom(from uint32, message string) {
	s.l.Printf("%d <- %d: %s", s.id, from, message)

}

func (s *stdLibLogger) LogTo(to uint32, message string) {
	s.l.Printf("%d -> %d: %s", s.id, to, message)
}

func NewSilentLogger() Logger {
	return &silentLogger{}
}

type silentLogger struct{}

func (sl *silentLogger) Log(message string) {}

func (sl *silentLogger) LogFrom(from uint32, message string) {}

func (sl *silentLogger) LogTo(to uint32, message string) {}
