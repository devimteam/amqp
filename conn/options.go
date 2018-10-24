package conn

import (
	"context"
	"time"

	"github.com/devimteam/amqp/logger"
	"github.com/streadway/amqp"
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

// WithContext allows use power of Context in connection loop.
// Common use-case: reconnection cancellation.
func WithContext(ctx context.Context) ConnectionOption {
	return func(connection *Connection) {
		connection.ctx = ctx
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

// Directly set dial config.
func WithConfig(config amqp.Config) ConnectionOption {
	return func(connection *Connection) {
		connection.config = config
	}
}

// Set custom connection name, that will be displayed in management plugins.
func Name(name string) ConnectionOption {
	return func(connection *Connection) {
		connection.config.Properties["connection_name"] = name
	}
}

// Set custom service name, that will be displayed in client information.
func Service(serviceName string) ConnectionOption {
	return func(connection *Connection) {
		connection.config.Properties["service_name"] = serviceName
	}
}

//
func Vhost(vhost string) ConnectionOption {
	return func(connection *Connection) {
		connection.config.Vhost = vhost
	}
}

func SASL(aa ...amqp.Authentication) ConnectionOption {
	return func(connection *Connection) {
		connection.config.SASL = append(connection.config.SASL, aa...)
	}
}
