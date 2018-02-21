package amqp

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/devimteam/amqp/codecs"
	"github.com/streadway/amqp"
)

type Client struct {
	cfgs configs
	opts options
	conn *Connection
}

func NewClient(conn *Connection, opts ...ClientOption) (cl Client) {
	cl.opts = defaultOptions()
	cl.cfgs = newConfigs()
	applyOptions(&cl, opts...)
	conn.WaitInit()
	return
}

func (c Client) Pub(ctx context.Context, exchangeName string, v interface{}, opts ...ClientOption) error {
	applyOptions(&c, opts...)
	if c.opts.waitConnection {
		err := c.conn.Wait(c.opts.waitConnectionDeadline)
		if err != nil {
			return err
		}
	}
	channel, err := c.conn.Connection().Channel()
	if err != nil {
		return WrapError("new channel", err)
	}
	err = channelExchangeDeclare(channel, exchangeName, c.cfgs.exchangeConfig)
	if err != nil {
		return WrapError("declare exchange", err)
	}
	msg, err := constructPublishing(v, &c.opts.msgOpts)
	if err != nil {
		return err
	}

	for _, before := range c.opts.msgOpts.pubBefore {
		before(ctx, &msg)
	}
	err = channelPublish(channel, exchangeName, c.cfgs.publishConfig, msg)
	if err != nil {
		return WrapError("publish", err)
	}
	return nil
}

func (c Client) Sub(exchangeName string, replyType interface{}, opts ...ClientOption) (<-chan Event, chan<- Signal) {
	eventChan := make(chan Event, c.opts.subEventChanBuffer)
	doneCh := make(chan Signal)
	go c.listen(exchangeName, replyType, eventChan, doneCh, opts...)
	return eventChan, doneCh
}

func (c Client) listen(exchangeName string, replyType interface{}, eventChan chan<- Event, doneChan <-chan Signal, opts ...ClientOption) {
	applyOptions(&c, opts...)
	if c.opts.waitConnection {
		err := c.conn.Wait(c.opts.waitConnectionDeadline)
		if err != nil {
			c.opts.eventLogger.Log(0, err)
		}
	}
	channel, err := c.conn.Connection().Channel()
	if err != nil {
		c.opts.eventLogger.Log(0, err)
		return
	}
	deliveryCh, queueName, err := c.prepareDeliveryChan(channel, exchangeName)
	if err != nil {
		c.opts.eventLogger.Log(0, err)
		return
	}
	c.processEvents(queueName, exchangeName, channel, deliveryCh, replyType, eventChan, doneChan)
	defer func() {
		select {
		case <-doneChan:
			return
		default:
			c.listen(exchangeName, replyType, eventChan, doneChan, opts...)
		}
	}()
}

func (c Client) prepareDeliveryChan(
	channel *amqp.Channel,
	exchangeName string,
) (<-chan amqp.Delivery, string, error) {
	c.opts.eventLogger.Log(3, fmt.Errorf("prepare delivery chan for exchange %s", exchangeName))
	c.opts.eventLogger.Log(3, fmt.Errorf("exchange(%s) declare", exchangeName))
	err := channelExchangeDeclare(channel, exchangeName, c.cfgs.exchangeConfig)
	if err != nil {
		return nil, "", fmt.Errorf("exchange declare err: %v", err)
	}
	c.opts.eventLogger.Log(3, fmt.Errorf("queue(%s) declare", c.cfgs.queueConfig.Name))
	q, err := channelQueueDeclare(channel, c.cfgs.queueConfig)
	if err != nil {
		return nil, "", fmt.Errorf("queue declare err: %v", err)
	}
	c.opts.eventLogger.Log(3, fmt.Errorf("bind queue(%s) to exchange(%s)", q.Name, exchangeName))
	err = channelQueueBind(channel, q.Name, exchangeName, c.cfgs.queueBindConfig)
	if err != nil {
		return nil, "", fmt.Errorf("queue bind err: %v", err)
	}
	c.opts.eventLogger.Log(3, fmt.Errorf("consume from queue(%s)", q.Name))
	ch, err := channelConsume(channel, q.Name, c.cfgs.consumeConfig)
	if err != nil {
		return nil, "", fmt.Errorf("channel consume err: %v", err)
	}
	return ch, q.Name, nil
}

func (c Client) processEvents(
	queueName, exchangeName string,
	channel *amqp.Channel,
	deliveryCh <-chan amqp.Delivery,
	replyType interface{},
	eventChan chan<- Event,
	doneChan <-chan Signal,
) {
	processedAll := false
	for {
		select {
		case d, ok := <-deliveryCh:
			if !ok {
				processedAll = true
				c.opts.eventLogger.Log(1, fmt.Errorf("delivery channel was closed"))
			}
			c.processEvent(d, replyType, eventChan)
		case <-doneChan:
			if c.opts.processAllDeliveries && processedAll {
				close(eventChan)
				if channel != nil {
					channel.QueueUnbind(queueName, c.cfgs.queueBindConfig.Key, exchangeName, c.cfgs.queueBindConfig.Args)
					channel.QueueDelete(queueName, c.cfgs.queueConfig.IfUnused, c.cfgs.queueConfig.IfEmpty, c.cfgs.queueConfig.NoWait)
					channel.Close()
				}
				return
			}
			// fixme: Should we sleep here? time.Sleep(time.Millisecond*100)
		}
	}
}

func (c Client) processEvent(d amqp.Delivery, replyType interface{}, eventChan chan<- Event) {
	err := c.checkEvent(d)
	if err != nil {
		c.opts.eventLogger.Log(1, err)
	}
	ev, err := c.handleEvent(d, replyType)
	if err != nil {
		e := d.Nack(false, true)
		if e != nil {
			c.opts.eventLogger.Log(1, fmt.Errorf("nack: %v", e))
		}
		c.opts.eventLogger.Log(2, fmt.Errorf("handle event: %v", err))
		return
	}
	eventChan <- ev
}

var NotAllowedPriority = errors.New("not allowed priority")

func (c Client) checkEvent(d amqp.Delivery) error {
	if c.opts.msgOpts.minPriority <= d.Priority && d.Priority <= c.opts.msgOpts.maxPriority {
		return NotAllowedPriority
	}
	return nil
}

func (c Client) handleEvent(d amqp.Delivery, replyType interface{}) (ev Event, err error) {
	ctx := c.opts.context
	for _, before := range c.opts.msgOpts.delBefore {
		ctx = before(ctx, &d)
	}
	ev.Context = ctx

	codec, ok := codecs.Register.Get(d.ContentType)
	if !ok {
		return ev, CodecNotFound
	}
	data := reflect.New(reflect.Indirect(reflect.ValueOf(replyType)).Type()).Interface()
	err = codec.Decode(d.Body, data)
	if err != nil {
		return
	}
	ev.Data = data
	return
}

type ClientOption func(*Client)

func applyOptions(cl *Client, opts ...ClientOption) {
	for i := range opts {
		opts[i](cl)
	}
}

func SetDefaultExchangeConfig(cfg ExchangeConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.exchangeConfig = cfg
	}
}

func SetDefaultQueueConfig(cfg QueueConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.queueConfig = cfg
	}
}

func SetDefaultQueueBindConfig(cfg QueueBindConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.queueBindConfig = cfg
	}
}

func SetDefaultConsumeConfig(cfg ConsumeConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.consumeConfig = cfg
	}
}

func SetDefaultPublishConfig(cfg PublishConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.publishConfig = cfg
	}
}

func WithOptions(opts ...Option) ClientOption {
	return func(client *Client) {
		for i := range opts {
			opts[i](&client.opts)
		}
	}
}
