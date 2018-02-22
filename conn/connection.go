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
	maxAttempts  int
}

type ConnectionOption func(*Connection)

func WithLogger(logger logger.Logger) ConnectionOption {
	return func(connection *Connection) {
		connection.logger = logger
	}
}

// WithDelayBuilder changes delay mechanism between attempts
func WithDelayBuilder(builder DelayBuilder) ConnectionOption {
	return func(connection *Connection) {
		connection.delayBuilder = builder
	}
}

// Timeout sets delays for connection between attempts.
func WithDelay(base, max time.Duration) ConnectionOption {
	return func(connection *Connection) {
		connection.delayBuilder = CommonDelayBuilder(max, base)
	}
}

// WithCancel gives ability to stop connection loop, when cancel channel closes or something sends to it.
func WithCancel(cancel <-chan Signal) ConnectionOption {
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
		maxAttempts:  -1,
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
func (c *Connection) connect(url string, dialer Dialer) {
	c.state.Disconnected()
	c.logger.Log(fmt.Errorf("disconnected from %s", url))
	delay := c.delayBuilder()
	i := 0
ConnectionLoop:
	for ; infinite(c.maxAttempts) || i < c.maxAttempts; i++ {
		delayCh := make(chan Signal)
		go func() {
			delay.Wait()
			delayCh <- Signal{}
			close(delayCh)
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
				c.logger.Log(fmt.Errorf("connection closed: %v", <-connection.NotifyClose(make(chan *amqp.Error))))
				c.notifier.Notify()
				c.connect(url, dialer)
			}()
			break ConnectionLoop
		}
	}
	defer c.state.Connected()
	if i == c.maxAttempts {
		c.logger.Log(MaxAttemptsError)
		return
	}
	defer c.logger.Log(fmt.Errorf("connected to %s", url))
}

var (
	DeadlineError    = errors.New("the deadline was reached")
	MaxAttemptsError = errors.New("maximum attempts was reached")
	CanceledError    = errors.New("connection was canceled")
)

func (c *Connection) Wait(timeout time.Duration) error {
	return Wait(func(r chan<- struct{}) {
		defer close(r)
		c.state.Disconnected()
		c.state.Connected()
		r <- struct{}{}
	}, timeout, DeadlineError)
}

// WaitInit can be called after one of Client constructors to ensure, that it is ready to serve.
func (c *Connection) WaitInit(timeout time.Duration) error {
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

// NotifyClose notifies user that connection was closed.
// Channel closes after first notification.
func (c *Connection) NotifyClose() <-chan Signal {
	ch := make(chan Signal)
	if c.state.IsConnected() {
		c.notifier.Register(ch)
	} else {
		go func() {
			ch <- Signal{}
		}()
	}
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
