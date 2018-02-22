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
