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
	config.Properties = setupDefaultConfigProperties(config.Properties)
	return DialWithDialer(func() (*amqp.Connection, error) { return amqp.DialConfig(url, config) }, opts...)
}

const (
	defaultProduct     = "https://github.com/devimteam/amqp"
	defaultVersion     = "v1.1.3"
	defaultPlatform    = "golang"
	defaultInformation = "github.com/devimteam/amqp is a wrapper of https://github.com/streadway/amqp"
)

func setupDefaultConfigProperties(prop amqp.Table) amqp.Table {
	if len(prop) == 0 {
		prop = amqp.Table{}
	}
	if _, ok := prop["product"]; !ok {
		prop["product"] = defaultProduct
	}
	if _, ok := prop["version"]; !ok {
		prop["version"] = defaultVersion
	}
	if _, ok := prop["platform"]; !ok {
		prop["platform"] = defaultPlatform
	}
	if _, ok := prop["information"]; !ok {
		prop["information"] = defaultInformation
	}
	return prop
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
