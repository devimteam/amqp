package conn

import (
	"time"

	"github.com/devimteam/amqp/logger"
)

// ConnectionOption is a type which represents optional Connection's feature.
type ConnectionOption func(*Connection)

// WithLogger sets logger, which notifies about these events:
func WithLogger(logger logger.Logger) ConnectionOption {
	return func(connection *Connection) {
		connection.logger = logger
	}
}

// WithDelayBuilder changes delay mechanism between attempts
func WithDelayBuilder(builder TimeoutBuilder) ConnectionOption {
	return func(connection *Connection) {
		connection.backoffer = builder
	}
}

// Timeout sets delays for connection between attempts.
func WithDelay(min, max time.Duration) ConnectionOption {
	return func(connection *Connection) {
		connection.backoffer = CommonTimeoutBuilder(min, max)
	}
}

// WithCancel gives ability to stop connection loop, when cancel channel closes or something sends to it.
func WithCancel(cancel chan Signal) ConnectionOption {
	return func(connection *Connection) {
		connection.done = cancel
	}
}

// Attempts sets the maximum attempts to connect/reconnect. When amount rises n, connection stops.
// When n < 0 Connection tries connect infinitely.
// -1 by default.
func Attempts(n int) ConnectionOption {
	return func(connection *Connection) {
		connection.maxAttempts = n
	}
}
