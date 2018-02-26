// Represents logger interface and common loggers.
package logger

// Logger represents logger interface that used to log messages.
// This interface is a copy of go-kit Logger interface.
type Logger interface {
	Log(v ...interface{}) error
}

var NoopLogger = noopLogger{}

type noopLogger struct{}

func (noopLogger) Log(v ...interface{}) error {
	return nil
}

type chanLogger struct {
	ch chan<- []interface{}
}

func NewChanLogger(ch chan<- []interface{}) Logger {
	return chanLogger{ch: ch}
}

func (l chanLogger) Log(v ...interface{}) error {
	l.ch <- v
	return nil
}
