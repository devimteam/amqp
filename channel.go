package amqp

import (
	"context"
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

	logger logger.Logger
}

func (c *Channel) publish(exchangeName string, msg amqp.Publishing, pub Publish) error {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	return c.channel.Publish(
		exchangeName,
		pub.Key,
		pub.Mandatory,
		pub.Immediate,
		msg,
	)
}

func (c *Channel) consume(queueName string, cfg Consumer) (<-chan amqp.Delivery, error) {
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

func (c *Channel) qos(count, size int) error {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	return c.channel.Qos(count, size, false)
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

func (c *Channel) keepalive(ctx context.Context, timeout time.Duration) {
Loop:
	for { // wait for success connection
		select {
		case <-ctx.Done():
			return
		default:
			if err := c.newChan(timeout); err != nil {
				_ = c.logger.Log(err)
				continue
			}
			break Loop
		}
	}
	c.callMx.Unlock()
	for ; !c.closed; c.callMx.Unlock() {
		select {
		case <-ctx.Done():
			if err := c.close(); err != nil {
				_ = c.logger.Log(err)
			}
			return
		case <-c.channel.NotifyClose(make(chan *amqp.Error)):
			c.callMx.Lock()
			if c.closed {
				break
			}
			if err := c.newChan(timeout); err != nil {
				_ = c.logger.Log(err)
				continue
			}
		}
	}
}

func (c *Channel) newChan(timeout time.Duration) error {
	err := c.conn.NotifyConnected(timeout)
	if err != nil {
		return err
	}
	channel, err := c.conn.Channel()
	if err != nil {
		return err
	}
	c.channel = channel
	return nil
}

func (c *Channel) close() error {
	c.callMx.Lock()
	defer c.callMx.Unlock()
	c.closed = true
	return c.channel.Close()
}
