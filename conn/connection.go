// Package conn adds to https://github.com/streadway/amqp Connection ability to reconnect and some optional parameters.
package conn

import (
	"context"
	"errors"
	"fmt"
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
		ctx         context.Context
		close       func()
		maxAttempts int
	}
)

func newConnection(opts ...ConnectionOption) *Connection {
	c := defaultConnection()
	for i := range opts {
		opts[i](&c)
	}
	c.ctx, c.close = context.WithCancel(c.ctx)
	return &c
}

func defaultConnection() Connection {
	return Connection{
		backoffer:   Backoffer(defaultMinInterval, defaultMaxInterval, defaultFactor, defaultJitter),
		logger:      logger.NoopLogger,
		ctx:         context.Background(),
		maxAttempts: -1,
		conn:        &amqp.Connection{},
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
		case <-c.ctx.Done():
			c.logger.Log(CanceledError)
			return
		case <-delayCh:
			delay.Inc()
			connection, err := dialer()
			if err != nil {
				c.logger.Log(fmt.Errorf("dialer: %v", err))
				continue
			}
			c.conn = connection
			go func() {
				select {
				case e := <-connection.NotifyClose(make(chan *amqp.Error)):
					c.logger.Log(fmt.Errorf("connection closed: %v", e))
					c.notifier.Notify()
					c.connect(dialer)
				case <-c.ctx.Done():
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

// NotifyClose notifies user that connection was closed.
// Channel closes after first notification.
func (c *Connection) NotifyClose() <-chan Signal {
	ch := make(chan Signal, 1)
	if c.state.isConnected() {
		c.notifier.Register(ch)
	} else {
		ch <- Signal{}
	}
	return ch
}

func (c *Connection) Close() error {
	c.close()
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
