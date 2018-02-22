package conn

import "time"

const defaultTimeoutBase = time.Second

type DelayBuilder func() Delayer

func CommonDelayBuilder(cap int, delay time.Duration) DelayBuilder {
	return func() Delayer {
		return CommonDelayer(cap, delay)
	}
}

type Delayer interface {
	Wait() // Wait for period of time.
	Inc()  // Increase duration of next Timeout call.
}

type delayer struct {
	cap        int
	currentCap uint
	baseDelay  time.Duration
}

func CommonDelayer(cap int, delay time.Duration) Delayer {
	return &delayer{
		currentCap: 0,
		cap:        cap,
		baseDelay:  delay,
	}
}

func (s *delayer) Wait() {
	time.Sleep(s.baseDelay * time.Duration(expOf2(s.currentCap)))
}

func (s *delayer) Inc() {
	if s.cap < 0 || s.currentCap < uint(s.cap) {
		s.currentCap++
	}
}

func (s *delayer) Value() time.Duration {
	return s.baseDelay * time.Duration(expOf2(s.currentCap))
}
