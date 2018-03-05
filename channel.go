package amqp

import (
	"sync"

	"time"

	"math"

	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
	"github.com/streadway/amqp"
)

// Channel is a wrapper of *amqp.Channel
type Channel struct {
	conn    *conn.Connection
	channel *amqp.Channel
	// Mutex to prevent multiple calls.
	callMx sync.Mutex
	closed bool

	declared declared
	logger   logger.Logger
}

type declared struct {
	exchanges map[string]ExchangeConfig
	queues    map[string]QueueConfig
	bindings  matrix
}

func (c *Channel) Publish(exchangeName string, msg amqp.Publishing, cfg PublishConfig) error {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	return c.channel.Publish(
		exchangeName,
		cfg.Key,
		cfg.Mandatory,
		cfg.Immediate,
		msg,
	)
}

func (c *Channel) Consume(queueName string, cfg ConsumeConfig) (<-chan amqp.Delivery, error) {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	return c.channel.Consume(
		queueName,
		cfg.Consumer,
		cfg.AutoAck,
		cfg.Exclusive,
		cfg.NoLocal,
		cfg.NoWait,
		cfg.Args,
	)
}

func (c *Channel) ExchangeDeclare(exchangeName string, exchangeCfg ExchangeConfig) (err error) {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	if _, ok := c.declared.exchanges[exchangeName]; ok {
		return nil
	}
	defer func() {
		if err == nil {
			c.declared.exchanges[exchangeName] = exchangeCfg
		}
	}()
	return c.exchangeDeclare(exchangeName, exchangeCfg)
}

func (c *Channel) exchangeDeclare(exchangeName string, exchangeCfg ExchangeConfig) (err error) {
	return c.channel.ExchangeDeclare(
		exchangeName,
		exchangeCfg.Kind,
		exchangeCfg.Durable,
		exchangeCfg.AutoDelete,
		exchangeCfg.Internal,
		exchangeCfg.NoWait,
		exchangeCfg.Args,
	)
}

func (c *Channel) QueueDeclare(queueCfg QueueConfig) (q amqp.Queue, err error) {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	if _, ok := c.declared.exchanges[queueCfg.Name]; queueCfg.Name != "" && ok {
		return amqp.Queue{Name: queueCfg.Name}, nil
	}
	defer func() {
		if queueCfg.Name != "" && err == nil {
			c.declared.queues[queueCfg.Name] = queueCfg
		}
	}()
	return c.queueDeclare(queueCfg)
}

func (c *Channel) queueDeclare(queueCfg QueueConfig) (q amqp.Queue, err error) {
	return c.channel.QueueDeclare(
		queueCfg.Name,
		queueCfg.Durable,
		queueCfg.AutoDelete,
		queueCfg.Exclusive,
		queueCfg.NoWait,
		queueCfg.Args,
	)
}

func (c *Channel) QueueBind(queueName, exchangeName string, queueBindCfg QueueBindConfig, temporary bool) (err error) {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	if !temporary {
		if _, ok := c.declared.bindings.Get(exchangeName, queueName); ok {
			return nil
		}
		c.declared.bindings.Set(exchangeName, queueName, queueBindCfg)
	}
	return c.queueBind(queueName, exchangeName, queueBindCfg)
}

func (c *Channel) queueBind(queueName, exchangeName string, queueBindCfg QueueBindConfig) (err error) {
	return c.channel.QueueBind(
		queueName,
		queueBindCfg.Key,
		exchangeName,
		queueBindCfg.NoWait,
		queueBindCfg.Args,
	)
}

func (c *Channel) keepalive(timeout time.Duration) {
	for ; !c.closed; c.callMx.Unlock() {
		select {
		case <-c.channel.NotifyClose(make(chan *amqp.Error)):
			continue
		case <-c.conn.NotifyClose():
			c.callMx.Lock()
			err := c.conn.NotifyConnected(timeout)
			if err != nil {
				// todo
			}
			channel, err := c.conn.Channel()
			if err != nil {
				// todo
			}
			c.channel = channel
			c.redeclare()
		}
	}
}

func (c *Channel) redeclare() {
	for k, v := range c.declared.exchanges {
		err := c.exchangeDeclare(k, v)
		if err != nil {
			c.logger.Log(err)
		}
	}
	for _, v := range c.declared.queues {
		_, err := c.queueDeclare(v)
		if err != nil {
			c.logger.Log(err)
		}
	}
	iter := c.declared.bindings.Iterator()
	for val := range iter {
		exchange, queue, cfg := val.x, val.y, val.v.(QueueBindConfig)
		err := c.queueBind(queue, exchange, cfg)
		if err != nil {
			c.logger.Log(err)
		}
	}
}

func (c *Channel) Close() error {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	c.closed = true
	return c.channel.Close()
}

type (
	ChannelsPool struct {
		conn         *conn.Connection
		m            sync.Mutex
		counter      chan struct{}
		idle         chan idleChan
		lastRevision time.Time
		options      channelsPoolOpts
		logger       logger.Logger
	}
	channelsPoolOpts struct {
		idleDuration time.Duration
		max          int
	}
	ChannelPoolOption func(opts *channelsPoolOpts)
)

func NewPool(conn *conn.Connection, options ...ChannelPoolOption) *ChannelsPool {
	opts := channelsPoolOpts{
		idleDuration: time.Minute * 2,
		max:          math.MaxUint16,
	}
	for _, o := range options {
		o(&opts)
	}
	pool := ChannelsPool{
		conn:         conn,
		idle:         make(chan idleChan, opts.max),
		counter:      make(chan struct{}, opts.max),
		lastRevision: time.Now(),
		options:      opts,
	}
	go func() {
		for range time.Tick(opts.idleDuration) {
			pool.clear()
		}
	}()
	return &pool
}

func (p *ChannelsPool) Channel() (*Channel, error) {
	p.m.Lock()
	defer p.m.Unlock()
	select {
	case idle := <-p.idle:
		return idle.ch, nil
	default: // go chooses case randomly, so we want to be sure, that we can choose idle channel firstly.
		select {
		case idle := <-p.idle:
			return idle.ch, nil
		case p.counter <- struct{}{}:
			ch := Channel{
				conn: p.conn,
				declared: declared{
					exchanges: make(map[string]ExchangeConfig),
					queues:    make(map[string]QueueConfig),
					bindings:  newMatrix(),
				},
				logger: p.logger,
			}
			go ch.keepalive(time.Minute)
			return &ch, nil
		}
	}
}

type idleChan struct {
	since time.Time
	ch    *Channel
}

func (p *ChannelsPool) clear() {
	p.m.Lock()
	var channels []idleChan
	revisionTime := time.Now()
	for c := range p.idle {
		if c.ch.closed {
			continue
		}
		if revisionTime.Sub(c.since) > p.options.idleDuration {
			c.ch.Close()
			continue
		}
		channels = append(channels)
	}
	for i := range channels {
		p.idle <- channels[i]
	}
	p.m.Unlock()
}

func (p *ChannelsPool) Release(ch *Channel) {
	if ch != nil {
		p.idle <- idleChan{since: time.Now(), ch: ch}
	}
}
