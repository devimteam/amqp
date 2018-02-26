package logger

func GoKitAdapter(logger Logger) Logger {
	return gokitAdapter{next: logger}
}

type gokitAdapter struct {
	next Logger
}

func (a gokitAdapter) Log(vals ...interface{}) error {
	if len(vals) == 1 {
		return a.next.Log("message", vals[0])
	}
	return a.next.Log()
}
