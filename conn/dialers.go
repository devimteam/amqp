package conn

import (
	"crypto/tls"
	"io"

	"github.com/streadway/amqp"
)

type (
	// Dialer setups connection to server.
	Dialer func() (*amqp.Connection, error)

	Connector func() (*Connection, error)
)

// DialWithDialer wraps any Dialer and adds reconnection ability.
// Never returns error.
func DialWithDialer(dialer Dialer, opts ...ConnectionOption) (*Connection, error) {
	c := newConnection(opts...)
	c.connect(dialer)
	return c, nil
}

// Dial wraps amqp.Dial function and adds reconnection ability.
// Never returns error.
func Dial(url string, opts ...ConnectionOption) (*Connection, error) {
	return DialWithDialer(func() (*amqp.Connection, error) { return amqp.Dial(url) }, opts...)
}

// DialTLS wraps amqp.DialTLS function and adds reconnection ability.
// Never returns error.
func DialTLS(url string, amqps *tls.Config, opts ...ConnectionOption) (*Connection, error) {
	return DialWithDialer(func() (*amqp.Connection, error) { return amqp.DialTLS(url, amqps) }, opts...)
}

// DialConfig wraps amqp.DialConfig function and adds reconnection ability.
// Never returns error.
func DialConfig(url string, config amqp.Config, opts ...ConnectionOption) (*Connection, error) {
	return DialWithDialer(func() (*amqp.Connection, error) { return amqp.DialConfig(url, config) }, opts...)
}

// Open wraps amqp.Open function and adds reconnection ability.
// Never returns error.
func Open(conn io.ReadWriteCloser, config amqp.Config, opts ...ConnectionOption) (*Connection, error) {
	return DialWithDialer(func() (*amqp.Connection, error) { return amqp.Open(conn, config) }, opts...)
}

func DefaultConnector(url string, opts ...ConnectionOption) Connector {
	return func() (*Connection, error) {
		return Dial(url, opts...)
	}
}

func ConfigConnector(url string, config amqp.Config, opts ...ConnectionOption) Connector {
	return func() (*Connection, error) {
		return DialConfig(url, config, opts...)
	}
}
