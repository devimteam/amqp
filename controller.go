package amqp

import (
	"time"

	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
	"github.com/streadway/amqp"
)

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func (e Exchange) declare(c *ClientV2) {
	if e.Name == "" {
		panic("exchange name can't be empty")
	}
	c.exchanges = append(c.exchanges, e)
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	//IfUnused   bool
	//IfEmpty    bool
	Args amqp.Table
}

func (q Queue) declare(c *ClientV2) {
	if q.Name == "" {
		panic("do not declare queue with empty name")
	}
	c.queues = append(c.queues, q)
}

type Binding struct {
	Exchange string
	Queue    string
	Key      string
	NoWait   bool
	Args     amqp.Table
}

func (b Binding) declare(c *ClientV2) {
	if b.Queue == "" || b.Exchange == "" {
		panic("empty exchange or queue name")
	}
	c.bindings = append(c.bindings, b)
}

type Consumer struct {
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type Publish struct {
	Key       string
	Mandatory bool
	Immediate bool
	Priority  uint8
}

type ClientV2 struct {
	exchanges []Exchange
	queues    []Queue
	bindings  []Binding
	conn      *conn.Connection
	observer  *observer
	connector conn.Connector
	logger    logger.Logger
}

func NewClientV2(connector conn.Connector, decls ...Declaration) (cl ClientV2, err error) {
	cl.constructorBefore(decls...)
	cl.connector = connector
	cl.conn, _ = connector()
	cl.observer = newObserver(cl.conn, Min(1), Max(1))
	cl.declare()
	go func() {
		for <-cl.conn.NotifyClose(); ; <-cl.conn.NotifyClose() {
			err := cl.conn.NotifyConnected(time.Minute)
			if err != nil {
				cl.logger.Log(err)
				continue
			}
			cl.declare()
		}
	}()
	return
}

func (c *ClientV2) constructorBefore(decls ...Declaration) {
	//c.opts = defaultOptions()
	//c.cfgs = newConfigs()
	withDeclarations(c, decls...)
}

func withDeclarations(cl *ClientV2, opts ...Declaration) {
	for i := range opts {
		opts[i].declare(cl)
	}
}

type Declaration interface {
	declare(*ClientV2)
}

func (c *ClientV2) declare() {
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

func (c *ClientV2) Subscriber(opts ...SubscriberOption) *Subscriber {
	var connection *conn.Connection
	connection = c.conn
	return newSubscriber(connection, opts...)
}

func (c *ClientV2) Publisher(opts ...PublisherOption) *Publisher {
	var connection *conn.Connection
	connection = c.conn
	return newPublisher(connection, opts...)
}
