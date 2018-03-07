// Package conn adds to https://github.com/streadway/amqp Connection ability to reconnect and some optional parameters.
package conn

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/devimteam/amqp/logger"
	"github.com/streadway/amqp"
)

type (
	// Connection is a wrapper of amqp.Connection with reconnection ability.
	Connection struct {
		conn        *amqp.Connection
		backoffer   TimeoutBuilder
		logger      logger.Logger
		state       connectionState
		notifier    Notifier
		done        chan Signal
		maxAttempts int
	}

	// ConnectionOption is a type which represents optional Connection's feature.
	ConnectionOption func(*Connection)
)

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

func newConnection(opts ...ConnectionOption) *Connection {
	c := defaultConnection()
	for i := range opts {
		opts[i](&c)
	}
	return &c
}

func defaultConnection() Connection {
	return Connection{
		backoffer:   Backoffer(defaultMinInterval, defaultMaxInterval, defaultFactor, defaultJitter),
		logger:      logger.NoopLogger,
		done:        make(chan Signal),
		maxAttempts: -1,
	}
}

// Connection gives direct access to amqp.Connection.
func (c *Connection) Connection() *amqp.Connection {
	return c.conn
}

// Connection gives direct access to amqp.Connection.
func (c *Connection) Channel() (*amqp.Channel, error) {
	return c.conn.Channel()
}

// Dialer setups connection to server.
type Dialer func() (*amqp.Connection, error)

// DialDialer wraps any Dialer and adds reconnection ability.
// Never returns error.
func DialDialer(dialer Dialer, opts ...ConnectionOption) (*Connection, error) {
	c := newConnection(opts...)
	c.connect(dialer)
	return c, nil
}

// Dial wraps amqp.Dial function and adds reconnection ability.
// Never returns error.
func Dial(url string, opts ...ConnectionOption) (*Connection, error) {
	return DialDialer(func() (*amqp.Connection, error) { return amqp.Dial(url) }, opts...)
}

// DialTLS wraps amqp.DialTLS function and adds reconnection ability.
// Never returns error.
func DialTLS(url string, amqps *tls.Config, opts ...ConnectionOption) (*Connection, error) {
	return DialDialer(func() (*amqp.Connection, error) { return amqp.DialTLS(url, amqps) }, opts...)
}

// DialConfig wraps amqp.DialConfig function and adds reconnection ability.
// Never returns error.
func DialConfig(url string, config amqp.Config, opts ...ConnectionOption) (*Connection, error) {
	return DialDialer(func() (*amqp.Connection, error) { return amqp.DialConfig(url, config) }, opts...)
}

// Open wraps amqp.Open function and adds reconnection ability.
// Never returns error.
func Open(conn io.ReadWriteCloser, config amqp.Config, opts ...ConnectionOption) (*Connection, error) {
	return DialDialer(func() (*amqp.Connection, error) { return amqp.Open(conn, config) }, opts...)
}

// connect connects with dialer and listens until connection closes.
// connect starts itself when connection drops.
func (c *Connection) connect(dialer Dialer) {
	c.state.disconnected()
	c.logger.Log(Disconnected)
	attemptNum, delay, delayCh := 0, c.backoffer(), make(chan Signal)
ConnectionLoop:
	for ; isInfinite(c.maxAttempts) || attemptNum < c.maxAttempts; attemptNum++ {
		close(delayCh)
		delayCh = make(chan Signal)
		go func() {
			delay.Wait()
			delayCh <- Signal{}
		}()
		select {
		case <-c.done:
			c.logger.Log(CanceledError)
			return
		case <-delayCh:
			delay.Inc()
			connection, err := dialer()
			if err != nil {
				c.logger.Log(fmt.Errorf("dial: %v", err))
				continue
			}
			c.conn = connection
			go func() {
				select {
				case e := <-connection.NotifyClose(make(chan *amqp.Error)):
					c.logger.Log(fmt.Errorf("connection closed: %v", e))
					c.notifier.Notify()
					c.connect(dialer)
				case <-c.done:
					c.logger.Log(CanceledError)
					connection.Close()
					c.notifier.Notify()
					return
				}
			}()
			break ConnectionLoop
		}
	}
	defer c.state.connected()
	if attemptNum == c.maxAttempts {
		c.logger.Log(MaxAttemptsError)
		return
	}
	defer c.logger.Log(Connected)
}

// Common errors
var (
	DeadlineError    = errors.New("the deadline was reached")
	MaxAttemptsError = errors.New("maximum attempts was reached")
	CanceledError    = errors.New("connection was canceled")
	Disconnected     = errors.New("disconnected")
	Connected        = errors.New("connected")
)

// NotifyConnected waits until connection is ready to serve.
func (c *Connection) NotifyConnected(timeout time.Duration) error {
	return timeoutPattern(func(r chan<- Signal) {
		defer close(r)
		c.state.disconnected()
		c.state.connected()
		r <- Signal{}
	}, timeout, DeadlineError)
}

// WaitInit can be called after one of Client constructors to ensure, that it is ready to serve.
func (c *Connection) WaitInit(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = time.Minute
	}
	return timeoutPattern(func(r chan<- Signal) {
		defer close(r)
		for c.conn == nil {
			time.Sleep(time.Millisecond * 100)
		}
		r <- Signal{}
	}, timeout, DeadlineError)
}

// NotifyClose notifies user that connection was closed.
// Channel closes after first notification.
func (c *Connection) NotifyClose() <-chan Signal {
	ch := make(chan Signal)
	if c.state.isConnected() {
		c.notifier.Register(ch)
	} else {
		go func() {
			ch <- Signal{}
		}()
	}
	return ch
}

func (c *Connection) Close() error {
	c.done <- Signal{}
	return c.conn.Close()
}

// Signal is shortcut for struct{}.
type Signal struct{}

// Notifier notify receivers when something happen.
// After notification it closes all channels
type Notifier struct {
	mx        sync.Mutex
	receivers []chan<- Signal
}

// Register registers r as receiver of Notify function.
func (d *Notifier) Register(r chan<- Signal) {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.receivers = append(d.receivers, r)
}

// Send signals to all receivers channels. After notification it closes notified channel.
// After each notification receiver should register again.
func (d *Notifier) Notify() {
	d.mx.Lock()
	defer d.mx.Unlock()
	for i := range d.receivers {
		d.receivers[i] <- Signal{}
		close(d.receivers[i])
	}
	d.receivers = []chan<- Signal{}
}

type connectionState struct {
	mx     sync.Mutex
	locked bool
}

func (s *connectionState) connected() {
	s.locked = false
	s.mx.Unlock()
}
func (s *connectionState) disconnected() {
	s.mx.Lock()
	s.locked = true
}
func (s *connectionState) isConnected() bool { return !s.locked }
