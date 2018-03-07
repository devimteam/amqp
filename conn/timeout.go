package conn

import (
	"math/rand"
	"time"
)

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

const (
	defaultMinInterval = time.Second
	defaultMaxInterval = time.Second * 60
	defaultFactor      = 1.5
	defaultJitter      = 0.5
)

type TimeoutBuilder func() Timeouter

func CommonTimeoutBuilder(min, max time.Duration) TimeoutBuilder {
	return func() Timeouter {
		return CommonTimeouter(min, max)
	}
}

func Backoffer(min, max time.Duration, factor, jitter float64) TimeoutBuilder {
	return func() Timeouter {
		return Backoff(min, max, factor, jitter)
	}
}

type Timeouter interface {
	Wait() // Wait for period of time.
	Inc()  // Increase duration of next Wait call.
}

type backoffer struct {
	current time.Duration

	min    time.Duration
	max    time.Duration
	factor float64
	jitter float64
}

func CommonTimeouter(min, max time.Duration) Timeouter {
	return &backoffer{
		current: 0,
		max:     max,
		min:     min,
		factor:  defaultFactor,
		jitter:  defaultJitter,
	}
}

func Backoff(min, max time.Duration, factor, jitter float64) Timeouter {
	return &backoffer{
		current: 0,
		min:     min,
		max:     max,
		factor:  factor,
		jitter:  jitter,
	}
}

func (s *backoffer) Wait() {
	time.Sleep(s.current)
}

func (s *backoffer) Inc() {
	s.current = max(s.current, s.min)
	if isInfiniteDuration(s.max) || s.current < s.max {
		s.current = min(s.next(), s.max)
		s.current = jitterDuration(s.current, s.jitter)
	}
}

func (s *backoffer) next() time.Duration {
	return s.current * time.Duration(s.factor)
}

func (s *backoffer) Value() time.Duration {
	return s.current
}

func isInfinite(n int) bool {
	return n < 0
}

func isInfiniteDuration(n time.Duration) bool {
	return n < 0
}

func min(a time.Duration, b time.Duration) time.Duration {
	if a > b {
		return b
	}
	return a
}

func max(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return b
	}
	return a
}

func jitterDuration(d time.Duration, jitter float64) time.Duration {
	return d + time.Duration(rand.Float64()*float64(d)*jitter)
}
