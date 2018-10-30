package metrics

// Counter describes a metric that accumulates values monotonically.
// An example of a counter is the number of received HTTP requests.
// This interface is a subset of related go-kit Counter interface.
type Counter interface {
	Add(delta float64)
}

// Gauge describes a metric that takes specific values over time.
// An example of a gauge is the current depth of a job queue.
// This interface is a subset of related go-kit Gauge interface.
type Gauge interface {
	Set(value float64)
}

var (
	NoopGauge   Gauge   = noop{}
	NoopCounter Counter = noop{}
)

type noop struct{}

func (c noop) Set(float64) {}
func (c noop) Add(float64) {}
