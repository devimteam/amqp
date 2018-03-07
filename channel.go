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
	exchanges map[string]*ExchangeConfig
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

func (c *Channel) ExchangeDeclare(exchangeName string, exchangeCfg *ExchangeConfig) (err error) {
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

func (c *Channel) exchangeDeclare(exchangeName string, exchangeCfg *ExchangeConfig) (err error) {
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
	for { // wait for success connection
		err := c.conn.NotifyConnected(timeout)
		if err != nil {
			c.logger.Log(err)
			continue
		}
		channel, err := c.conn.Channel()
		if err != nil {
			c.logger.Log(err)
			continue
		}
		c.channel = channel
		break
	}
	c.callMx.Unlock()
	for ; !c.closed; c.callMx.Unlock() {
		select {
		case <-c.channel.NotifyClose(make(chan *amqp.Error)):
			c.callMx.Lock()
			if c.closed {
				break
			}
			err := c.conn.NotifyConnected(timeout)
			if err != nil {
				c.logger.Log(err)
				continue
			}
			channel, err := c.conn.Channel()
			if err != nil {
				c.logger.Log(err)
				continue
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

func (c *Channel) close() error {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	c.closed = true
	return c.channel.Close()
}

type (
	observer struct {
		conn         *conn.Connection
		m            sync.Mutex
		counter      chan struct{}
		count        int
		idle         chan idleChan
		lastRevision time.Time
		options      observerOpts
		logger       logger.Logger
	}
	observerOpts struct {
		idleDuration time.Duration
		min          int
		max          int
	}
	ObserverOption func(opts *observerOpts)
)

// Max sets maximum amount of channels, that can be opened at the same time.
func Max(max int) ObserverOption {
	return func(opts *observerOpts) {
		opts.max = max
	}
}

// Min sets minimum amount of channels, that should be opened at the same time.
// Min does not open new channels, but forces observer not to close existing ones.
func Min(min int) ObserverOption {
	return func(opts *observerOpts) {
		opts.min = min
	}
}

// Lifetime sets duration between observer checks idle channels.
// Somewhere between dur and 2*dur observer will close channels, which do not used at least `dur` time units.
// Default value is 15 seconds.
func Lifetime(dur time.Duration) ObserverOption {
	return func(opts *observerOpts) {
		opts.idleDuration = dur
	}
}

const defaultChannelIdleDuration = time.Second * 15

func newObserver(conn *conn.Connection, options ...ObserverOption) *observer {
	opts := observerOpts{
		idleDuration: defaultChannelIdleDuration,
		min:          0,
		max:          math.MaxUint16, // From https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf, section 4.9 Limitations
	}
	for _, o := range options {
		o(&opts)
	}
	pool := observer{
		conn:         conn,
		idle:         make(chan idleChan, opts.max),
		counter:      make(chan struct{}, opts.max),
		count:        0,
		lastRevision: time.Now(),
		options:      opts,
		logger:       logger.NoopLogger,
	}
	go func() {
		for {
			time.Sleep(opts.idleDuration)
			pool.clear()
		}
	}()
	return &pool
}

func (p *observer) channel() *Channel {
	p.m.Lock()
	defer p.m.Unlock()
	select {
	case idle := <-p.idle:
		return idle.ch
	default: // Go chooses case randomly, so we want to be sure, that we can choose idle channel firstly.
		select {
		case idle := <-p.idle:
			return idle.ch
		case p.counter <- struct{}{}:
			p.count++
			ch := Channel{
				conn: p.conn,
				declared: declared{
					exchanges: make(map[string]*ExchangeConfig),
					queues:    make(map[string]QueueConfig),
					bindings:  newMatrix(),
				},
				logger: p.logger,
			}
			ch.callMx.Lock() // Lock to prevent calls on nil channel. Mutex should be unlocked in `keepalive` function.
			go ch.keepalive(time.Minute)
			return &ch
		}
	}
}

type idleChan struct {
	since time.Time
	ch    *Channel
}

func (p *observer) clear() {
	p.m.Lock()
	var channels []idleChan
	revisionTime := time.Now()
Loop:
	for {
		select {
		case c := <-p.idle:
			if c.ch.closed {
				p.count--
				continue
			}
			if revisionTime.Sub(c.since) > p.options.idleDuration && p.count < p.options.min {
				c.ch.close()
				p.count--
				continue
			}
			channels = append(channels, c)
		default:
			break Loop
		}
	}
	for i := range channels {
		p.idle <- channels[i]
	}
	p.lastRevision = revisionTime
	p.m.Unlock()
}

func (p *observer) shouldBeClosed(revisionTime time.Time, c *idleChan) bool {
	return revisionTime.Sub(c.since) > p.options.idleDuration && p.count > p.options.min
}

func (p *observer) release(ch *Channel) {
	if ch != nil {
		p.idle <- idleChan{since: time.Now(), ch: ch}
	}
}
