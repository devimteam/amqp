package conn

import "time"

func timeoutPattern(fn func(chan<- Signal), timeout time.Duration, deadlineErr error) error {
	r := make(chan Signal)
	go fn(r)
	select {
	case <-r:
		return nil
	case <-time.After(timeout):
		return deadlineErr
	}
}

const defaultTimeoutBase = time.Second

type TimeoutBuilder func() Timeouter

func CommonTimeoutBuilder(max, min time.Duration) TimeoutBuilder {
	return func() Timeouter {
		return CommonTimeouter(max, min)
	}
}

type Timeouter interface {
	Wait() // Wait for period of time.
	Inc()  // Increase duration of next Wait call.
}

type backoffer struct {
	max       time.Duration
	current   time.Duration
	baseDelay time.Duration
}

func CommonTimeouter(max, min time.Duration) Timeouter {
	return &backoffer{
		current:   0,
		max:       max,
		baseDelay: min,
	}
}

func (s *backoffer) Wait() {
	time.Sleep(s.current)
}

func (s *backoffer) Inc() {
	if isInfinite(int(s.max)) || s.current < s.max {
		s.current *= 2
	}
	if s.current == 0 {
		s.current = s.baseDelay
	}
}

func (s *backoffer) Value() time.Duration {
	return s.current
}

func isInfinite(n int) bool {
	return n < 0
}
