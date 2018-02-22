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

// Connection is a wrapper of amqp.Connection with reconnection ability.
type Connection struct {
	conn         *amqp.Connection
	delayBuilder DelayBuilder
	logger       logger.Logger
	state        ConnectionState
	notifier     Notifier
	done         <-chan Signal
}

type ConnectionOption func(*Connection)

func WithLogger(logger logger.Logger) ConnectionOption {
	return func(connection *Connection) {
		connection.logger = logger
	}
}

func WithDelayBuilder(builder DelayBuilder) ConnectionOption {
	return func(connection *Connection) {
		connection.delayBuilder = builder
	}
}

// Timeout sets delays for connection between attempts.
func WithTimeout(base time.Duration, cap int) ConnectionOption {
	return func(connection *Connection) {
		connection.delayBuilder = CommonDelayBuilder(cap, base)
	}
}

// WithCancel gives ability to stop connection loop, when cancel channel closes or something sends to it.
func WithCancel(cancel <-chan Signal) ConnectionOption {
	return func(connection *Connection) {
		connection.done = cancel
	}
}

func newConnection(opts ...ConnectionOption) Connection {
	c := defaultConnection()
	for i := range opts {
		opts[i](&c)
	}
	return c
}

func defaultConnection() Connection {
	return Connection{
		delayBuilder: CommonDelayBuilder(-1, defaultTimeoutBase),
		logger:       logger.NoopLogger,
		done:         make(chan Signal),
	}
}

type Dialer func() (*amqp.Connection, error)

func (c Connection) Connection() *amqp.Connection {
	return c.conn
}

// Dial wraps amqp.Dial function and adds reconnection ability.
// Never returns error.
func Dial(url string, opts ...ConnectionOption) (*Connection, error) {
	c := newConnection(opts...)
	c.connect(url, func() (*amqp.Connection, error) {
		return amqp.Dial(url)
	})
	return &c, nil
}

// Dial wraps amqp.Dial function and adds reconnection ability.
// Never returns error.
func DialTLS(url string, amqps *tls.Config, opts ...ConnectionOption) (*Connection, error) {
	c := newConnection(opts...)
	c.connect(url, func() (*amqp.Connection, error) {
		return amqp.DialTLS(url, amqps)
	})
	return &c, nil
}

// Dial wraps amqp.Dial function and adds reconnection ability.
// Never returns error.
func DialConfig(url string, config amqp.Config, opts ...ConnectionOption) (*Connection, error) {
	c := newConnection(opts...)
	c.connect(url, func() (*amqp.Connection, error) {
		return amqp.DialConfig(url, config)
	})
	return &c, nil
}

// Dial wraps amqp.Dial function and adds reconnection ability.
// Never returns error.
func Open(conn io.ReadWriteCloser, config amqp.Config, opts ...ConnectionOption) (*Connection, error) {
	c := newConnection(opts...)
	c.connect("", func() (*amqp.Connection, error) {
		return amqp.Open(conn, config)
	})
	return &c, nil
}

// connect connects with dialer and listens until connection drops.
// connect starts itself when connection drops.
func (c Connection) connect(url string, dialer Dialer) {
	c.state.Disconnected()
	c.logger.Log(2, fmt.Errorf("disconnected from %s", url))
	timeout := c.delayBuilder()
	for {
		select {
		case <-c.done:
			return
		default:
			timeout.Wait()
			timeout.Inc()
			connection, err := dialer()
			if err != nil {
				c.logger.Log(0, fmt.Errorf("dial: %v", err))
				continue
			}
			c.conn = connection
			go func() {
				c.logger.Log(0, fmt.Errorf("connection closed: %v", <-connection.NotifyClose(make(chan *amqp.Error))))
				c.notifier.Notify()
				c.connect(url, dialer)
			}()
			break
		}
	}
	defer c.state.Connected()
	defer c.logger.Log(2, fmt.Errorf("connected to %s", url))
}

var DeadlineError = errors.New("the deadline was reached")

func (c Connection) Wait(timeout time.Duration) error {
	return Wait(func(r chan<- struct{}) {
		defer close(r)
		c.state.Disconnected()
		c.state.Connected()
		r <- struct{}{}
	}, timeout, DeadlineError)
}

// WaitInit can be called after one of Client constructors to ensure, that it is ready to serve.
func (c Connection) WaitInit(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = time.Hour
	}
	return Wait(func(r chan<- struct{}) {
		defer close(r)
		for c.conn == nil {
			time.Sleep(time.Millisecond * 100)
		}
		r <- struct{}{}
	}, timeout, DeadlineError)
}

func (c Connection) NotifyClose() <-chan Signal {
	ch := make(chan Signal)
	c.notifier.Register(ch)
	return ch
}

type ConnectionState struct {
	mx     sync.Mutex
	locked bool
}

func (s *ConnectionState) Connected() {
	s.locked = false
	s.mx.Unlock()
}

func (s *ConnectionState) Disconnected() {
	s.mx.Lock()
	s.locked = true
}

func (s *ConnectionState) IsConnected() bool {
	return !s.locked
}

func Wait(fn func(chan<- struct{}), timeout time.Duration, deadlineErr error) error {
	r := make(chan struct{})
	go fn(r)
	select {
	case <-r:
		return nil
	case <-time.After(timeout):
		return deadlineErr
	}
}

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
