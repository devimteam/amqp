package amqp

import (
	"sync"
	"time"

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
	limits struct {
		count int
		size  int
	}
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

func (c *Channel) declareExchange(exchange Exchange) error {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	return c.channel.ExchangeDeclare(
		exchange.Name,
		exchange.Kind,
		exchange.Durable,
		exchange.AutoDelete,
		exchange.Internal,
		exchange.NoWait,
		exchange.Args,
	)
}

func (c *Channel) declareQueue(queue Queue) (amqp.Queue, error) {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	return c.channel.QueueDeclare(
		queue.Name,
		queue.Durable,
		queue.AutoDelete,
		queue.Exclusive,
		queue.NoWait,
		queue.Args,
	)
}

func (c *Channel) bind(binding Binding) error {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	return c.channel.QueueBind(
		binding.Queue,
		binding.Key,
		binding.Exchange,
		binding.NoWait,
		binding.Args,
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
		c.redeclare()
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
	if c.declared.limits.count > 0 || c.declared.limits.size > 0 {
		err := c.channel.Qos(c.declared.limits.count, c.declared.limits.size, false)
		if err != nil {
			c.logger.Log(err)
		}
	}
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
