package amqp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/devimteam/amqp/codecs"
	"github.com/devimteam/amqp/conn"
	"github.com/streadway/amqp"
)

type Client struct {
	cfgs configs
	opts options
	conn *conn.Connection
	lazy struct {
		exchangesDeclared SyncedStringSlice
		queueDeclared     SyncedStringSlice
	}
}

func NewClientWithConnection(conn *conn.Connection, opts ...ClientOption) (cl Client) {
	cl.opts = defaultOptions()
	cl.cfgs = newConfigs()
	cl.conn = conn
	applyOptions(&cl, opts...)
	conn.WaitInit(0)
	return
}

func NewClient(url string, opts ...ClientOption) (cl Client) {
	cl.opts = defaultOptions()
	cl.cfgs = newConfigs()
	applyOptions(&cl, opts...)
	if cl.cfgs.conn != nil {
		cl.conn, _ = conn.DialConfig(url, *cl.cfgs.conn, cl.opts.connOpts...)
	} else {
		cl.conn, _ = conn.Dial(url, cl.opts.connOpts...)
	}
	return
}

func (c Client) Pub(ctx context.Context, exchangeName string, v interface{}, opts ...ClientOption) error {
	applyOptions(&c, opts...)
	if c.opts.wait.flag {
		err := c.conn.Wait(c.opts.wait.timeout)
		if err != nil {
			return err
		}
	}
	channel, err := c.conn.Connection().Channel()
	if err != nil {
		return WrapError("new channel", err)
	}
	err = c.channelExchangeDeclare(channel, exchangeName, c.cfgs.exchange)
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
	err = channelPublish(channel, exchangeName, c.cfgs.publish, msg)
	if err != nil {
		return WrapError("publish", err)
	}
	return nil
}

func (c Client) Sub(exchangeName string, replyType interface{}, opts ...ClientOption) (<-chan Event, chan<- conn.Signal) {
	eventChan := make(chan Event, c.opts.subEventChanBuffer)
	doneCh := make(chan conn.Signal)
	go c.listen(exchangeName, replyType, eventChan, doneCh, opts...)
	return eventChan, doneCh
}

func (c Client) listen(exchangeName string, replyType interface{}, eventChan chan<- Event, doneChan <-chan conn.Signal, opts ...ClientOption) {
	applyOptions(&c, opts...)
	if c.opts.wait.flag {
		err := c.conn.Wait(c.opts.wait.timeout)
		if err != nil {
			c.opts.log.error.Log(err)
		}
	}
	channel, err := c.conn.Connection().Channel()
	if err != nil {
		c.opts.log.error.Log(err)
		return
	}
	deliveryCh, queueName, err := c.prepareDeliveryChan(channel, exchangeName)
	if err != nil {
		c.opts.log.error.Log(err)
		return
	}
	c.processersPool(queueName, exchangeName, channel, deliveryCh, replyType, eventChan, doneChan)
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
	c.opts.log.debug.Log(fmt.Errorf("prepare delivery chan for exchange %s", exchangeName))
	c.opts.log.debug.Log(fmt.Errorf("exchange(%s) declare", exchangeName))
	err := c.channelExchangeDeclare(channel, exchangeName, c.cfgs.exchange)
	if err != nil {
		return nil, "", fmt.Errorf("exchange declare err: %v", err)
	}
	c.opts.log.debug.Log(fmt.Errorf("queue(%s) declare", c.cfgs.queue.Name))
	q, err := c.channelQueueDeclare(channel, c.cfgs.queue)
	if err != nil {
		return nil, "", fmt.Errorf("queue declare err: %v", err)
	}
	c.opts.log.debug.Log(fmt.Errorf("bind queue(%s) to exchange(%s)", q.Name, exchangeName))
	err = channelQueueBind(channel, q.Name, exchangeName, c.cfgs.queueBind)
	if err != nil {
		return nil, "", fmt.Errorf("queue bind err: %v", err)
	}
	c.opts.log.debug.Log(fmt.Errorf("consume from queue(%s)", q.Name))
	ch, err := channelConsume(channel, q.Name, c.cfgs.consume)
	if err != nil {
		return nil, "", fmt.Errorf("channel consume err: %v", err)
	}
	return ch, q.Name, nil
}

func (c Client) processersPool(
	queueName, exchangeName string,
	channel *amqp.Channel,
	deliveryCh <-chan amqp.Delivery,
	replyType interface{},
	eventChan chan<- Event,
	doneChan <-chan conn.Signal,
) {
	var wg sync.WaitGroup
	wg.Add(c.opts.handlersAmount)
	for i := 0; i < c.opts.handlersAmount; i++ {
		go func() {
			c.processEvents(queueName, exchangeName, channel, deliveryCh, replyType, eventChan, doneChan)
			wg.Done()
		}()
	}
	wg.Wait()
}

var DeliveryChannelWasClosedError = errors.New("delivery channel was closed")

func (c Client) processEvents(
	queueName, exchangeName string,
	channel *amqp.Channel,
	deliveryCh <-chan amqp.Delivery,
	replyType interface{},
	eventChan chan<- Event,
	doneChan <-chan conn.Signal,
) {
	processedAll := false
	for {
		select {
		case d, ok := <-deliveryCh:
			if !ok {
				processedAll = true
				c.opts.log.info.Log(DeliveryChannelWasClosedError)
				return
			}
			c.opts.log.debug.Log(fmt.Errorf("process delivery %s", d.MessageId))
			c.processEvent(d, replyType, eventChan)
		case <-doneChan:
			if c.opts.processAllDeliveries && processedAll {
				close(eventChan)
				if channel != nil {
					channel.QueueUnbind(queueName, c.cfgs.queueBind.Key, exchangeName, c.cfgs.queueBind.Args)
					channel.QueueDelete(queueName, c.cfgs.queue.IfUnused, c.cfgs.queue.IfEmpty, c.cfgs.queue.NoWait)
					channel.Close()
				}
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c Client) processEvent(d amqp.Delivery, replyType interface{}, eventChan chan<- Event) {
	err := c.checkEvent(d)
	if err != nil {
		err = c.errorBefore(d, err)
		c.opts.log.warn.Log(err)
		e := d.Nack(false, true)
		if e != nil {
			c.opts.log.error.Log(fmt.Errorf("nack delivery: %v because of %v", e, err))
		}
		return
	}
	ev, err := c.handleEvent(d, replyType)
	if err != nil {
		err = c.errorBefore(d, err)
		c.opts.log.warn.Log(err)
		e := d.Nack(false, true)
		if e != nil {
			c.opts.log.error.Log(fmt.Errorf("nack delivery: %v because of %v", e, err))
		}
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

// ErrorBefore allows user to update error messages before logging.
func (c Client) errorBefore(d amqp.Delivery, err error) error {
	for _, before := range c.opts.errorBefore {
		err = before(d, err)
	}
	return err
}

type ClientOption func(*Client)

func applyOptions(cl *Client, opts ...ClientOption) {
	for i := range opts {
		opts[i](cl)
	}
}

func SetDefaultExchangeConfig(cfg ExchangeConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.exchange = cfg
	}
}

func SetDefaultQueueConfig(cfg QueueConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.queue = cfg
	}
}

func SetDefaultQueueBindConfig(cfg QueueBindConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.queueBind = cfg
	}
}

func SetDefaultConsumeConfig(cfg ConsumeConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.consume = cfg
	}
}

func SetDefaultPublishConfig(cfg PublishConfig) ClientOption {
	return func(client *Client) {
		client.cfgs.publish = cfg
	}
}

// Has no effect on NewClientWithConnection, Client.Sub and Client.Pub calls.
func WithConfig(config amqp.Config) ClientOption {
	return func(client *Client) {
		client.cfgs.conn = &config
	}
}

func WithOptions(opts ...Option) ClientOption {
	return func(client *Client) {
		for i := range opts {
			opts[i](&client.opts)
		}
	}
}

// Has no effect on NewClientWithConnection function.
func WithConnOptions(opts ...conn.ConnectionOption) ClientOption {
	return func(client *Client) {
		client.opts.connOpts = append(client.opts.connOpts, opts...)
	}
}
