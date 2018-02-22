package conn

import "time"

const defaultTimeoutBase = time.Second

type DelayBuilder func() Delayer

func CommonDelayBuilder(max, min time.Duration) DelayBuilder {
	return func() Delayer {
		return CommonDelayer(max, min)
	}
}

type Delayer interface {
	Wait() // Wait for period of time.
	Inc()  // Increase duration of next Wait call.
}

type delayer struct {
	max       time.Duration
	current   time.Duration
	baseDelay time.Duration
}

func CommonDelayer(max, min time.Duration) Delayer {
	return &delayer{
		current:   0,
		max:       max,
		baseDelay: min,
	}
}

func (s *delayer) Wait() {
	time.Sleep(s.current)
}

func (s *delayer) Inc() {
	if infinite(int(s.max)) || s.current < s.max {
		s.current *= 2
	}
	if s.current == 0 {
		s.current = s.baseDelay
	}
}

func (s *delayer) Value() time.Duration {
	return s.current
}

func infinite(n int) bool {
	return n < 0
}
