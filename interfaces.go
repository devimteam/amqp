package amqp

type Logger interface {
	Log(level int, err error)
}

type ContentTyper interface {
	ContentType() string
}

type noopLogger struct{}

func (noopLogger) Log(int, error) {
	return
}
