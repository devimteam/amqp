package amqp

import (
	"context"
	"errors"
	"time"

	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
	"github.com/streadway/amqp"
)

var (
	// This error occurs when message was delivered, but it has too low or too high priority.
	NotAllowedPriority = errors.New("not allowed priority")
	// DeliveryChannelWasClosedError is an information error, that logs to info logger when delivery channel was closed.
	DeliveryChannelWasClosedError = errors.New("delivery channel was closed")
	// Durable or non-auto-delete queues with empty names will survive when all consumers have finished using it, but no one can connect to it back.
	QueueDeclareWarning = errors.New("declaring durable or non-auto-delete queue with empty name")
)

// Event represents amqp.Delivery with attached context and data
type Event struct {
	// Converted and ready to use pointer to entity of reply type.
	Data interface{}
	// Event's context.
	// Contains context.Background by default and setups with DeliveryBefore option.
	Context context.Context
	amqp.Delivery
}

// Done is a shortcut for Ack(false)
func (e Event) Done() {
	e.Ack(false)
}

type Client struct {
	exchanges []Exchange
	queues    []Queue
	bindings  []Binding
	conn      *conn.Connection
	observer  *observer
	logger    logger.Logger
	done      func()
	ctx       context.Context
}

func NewClient(connector conn.Connector, decls ...Declaration) (cl Client, err error) {
	cl.constructorBefore(decls...)
	ctx, done := context.WithCancel(context.Background())
	cl.done = done
	cl.ctx = ctx
	cl.conn, err = connector()
	if err != nil {
		return
	}
	cl.observer = newObserver(ctx, cl.conn, Max(1)) // We need only one channel to declare all queues and exchanges.
	cl.declare()
	go func() {
		for <-cl.conn.NotifyClose(); ; <-cl.conn.NotifyClose() {
			select {
			case <-ctx.Done():
				return
			default:
				err := cl.conn.NotifyConnected(time.Minute)
				if err != nil {
					cl.logger.Log(err)
					continue
				}
				cl.declare()
			}
		}
	}()
	return
}

func (c *Client) constructorBefore(decls ...Declaration) {
	c.logger = logger.NoopLogger
	withDeclarations(c, decls...)
}

func withDeclarations(cl *Client, opts ...Declaration) {
	for i := range opts {
		opts[i].declare(cl)
	}
}

type Declaration interface {
	declare(*Client)
}

func (c *Client) declare() {
	ch := c.observer.channel()
	for _, exchange := range c.exchanges {
		err := ch.declareExchange(exchange)
		if err != nil {
			c.logger.Log(err)
		}
	}
	for _, queue := range c.queues {
		warn := checkQueue(queue)
		if warn != nil {
			c.logger.Log(warn)
		}
		_, err := ch.declareQueue(queue)
		if err != nil {
			c.logger.Log(err)
		}
	}
	for _, binding := range c.bindings {
		err := ch.bind(binding)
		if err != nil {
			c.logger.Log(err)
		}
	}
	c.observer.release(ch)
}

func checkQueue(queue Queue) error {
	if queue.Name == "" && (queue.Durable || !queue.AutoDelete) {
		return QueueDeclareWarning
	}
	return nil
}

func (c *Client) Subscriber(opts ...SubscriberOption) *Subscriber {
	var connection *conn.Connection
	connection = c.conn
	return newSubscriber(c.ctx, connection, opts...)
}

func (c *Client) Publisher(opts ...PublisherOption) *Publisher {
	var connection *conn.Connection
	connection = c.conn
	return newPublisher(c.ctx, connection, opts...)
}

func (c *Client) Stop() {
	c.done()
}

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func (e Exchange) declare(c *Client) {
	if e.Name == "" {
		panic("exchange name can't be empty")
	}
	c.exchanges = append(c.exchanges, e)
}

// TemporaryExchange is a common way to create temporary exchange with given name.
func TemporaryExchange(name string) Exchange {
	return Exchange{
		Name:       name,
		Kind:       "fanout",
		Durable:    false,
		AutoDelete: true,
		Internal:   false,
		NoWait:     false,
	}
}

// LongExchange is a common way to declare exchange with given name.
func LongExchange(name string) Exchange {
	return Exchange{
		Name:       name,
		Kind:       "fanout",
		Durable:    true,
		AutoDelete: false,
	}
}

type Exchanges []Exchange

func (e Exchanges) declare(c *Client) {
	for i := range e {
		if e[i].Name == "" {
			panic("exchange name can't be empty")
		}
		c.exchanges = append(c.exchanges, e[i])
	}
}

// PersistentExchanges allow you to declare a bunch of exchanges with given names.
func PersistentExchanges(names ...string) (e Exchanges) {
	for i := range names {
		e = append(e, LongExchange(names[i]))
	}
	return
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// LongQueue is a common way to declare queue with given name.
func LongQueue(name string) Queue {
	return Queue{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
	}
}

func (q Queue) declare(c *Client) {
	if q.Name == "" {
		panic("do not declare queue with empty name")
	}
	c.queues = append(c.queues, q)
}

type Queues []Queue

func (q Queues) declare(c *Client) {
	for i := range q {
		if q[i].Name == "" {
			panic("queue name can't be empty")
		}
		c.queues = append(c.queues, q[i])
	}
}

// PersistentQueues allow you to declare a bunch of queues with given names.
func PersistentQueues(names ...string) (e Queues) {
	for i := range names {
		e = append(e, LongQueue(names[i]))
	}
	return
}

// Binding is used for bind exchange and queue.
type Binding struct {
	Exchange string
	Queue    string
	Key      string
	NoWait   bool
	Args     amqp.Table
}

func (b Binding) declare(c *Client) {
	if b.Queue == "" || b.Exchange == "" {
		panic("empty exchange or queue name")
	}
	c.bindings = append(c.bindings, b)
}

type Consumer struct {
	Consumer   string
	AutoAck    bool
	Exclusive  bool
	NoLocal    bool
	NoWait     bool
	Args       amqp.Table
	LimitCount int
	LimitSize  int
}

// Publish is used for AMQP Publish parameters.
type Publish struct {
	Key       string
	Mandatory bool
	Immediate bool
	Priority  uint8
}

// WithLogger set logger for client, which will report declaration problems and so on.
type WithLogger struct {
	logger.Logger
}

func (b WithLogger) declare(c *Client) {
	c.logger = b.Logger
}
