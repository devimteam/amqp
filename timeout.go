package amqp

import "time"

const defaultTimeoutBase = time.Second

type TimeoutBuilder func() Timeouter

func DefaultTimeoutBuilder(cap int, delay time.Duration) TimeoutBuilder {
	return func() Timeouter {
		return NewTimeouter(cap, delay)
	}
}

type Timeouter interface {
	Timeout() // Wait for period of time.
	Inc()     // Increase duration of next Timeout call.
}

type timeouter struct {
	cap        int
	currentCap uint
	baseDelay  time.Duration
}

func NewTimeouter(cap int, delay time.Duration) *timeouter {
	return &timeouter{
		currentCap: 0,
		cap:        cap,
		baseDelay:  delay,
	}
}

func (s *timeouter) Timeout() {
	time.Sleep(s.baseDelay * time.Duration(expOf2(s.currentCap)))
}

func (s *timeouter) Inc() {
	if s.cap < 0 || s.currentCap < uint(s.cap) {
		s.currentCap++
	}
}

func (s *timeouter) Value() time.Duration {
	return s.baseDelay * time.Duration(expOf2(s.currentCap))
}
