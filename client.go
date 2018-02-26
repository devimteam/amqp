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

type Event struct {
	Data    interface{}     // converted and ready to use entity of reply type.
	Context context.Context // event's context.
	amqp.Delivery
}

func (e Event) Done() {
	e.Ack(false)
}

type Client struct {
	cfgs configs
	opts options
	conn *conn.Connection
}

func NewClientWithConnection(conn *conn.Connection, cfgs ...ClientConfig) (cl Client) {
	cl.opts = defaultOptions()
	cl.cfgs = newConfigs()
	cl.conn = conn
	applyConfigs(&cl, cfgs...)
	conn.WaitInit(0)
	return
}

func NewClient(url string, cfgs ...ClientConfig) (cl Client, err error) {
	cl.opts = defaultOptions()
	cl.cfgs = newConfigs()
	applyConfigs(&cl, cfgs...)
	if cl.cfgs.conn != nil {
		cl.conn, _ = conn.DialConfig(url, *cl.cfgs.conn, cl.opts.connOpts...)
	} else {
		cl.conn, _ = conn.Dial(url, cl.opts.connOpts...)
	}
	err = cl.conn.WaitInit(0)
	if cl.opts.lazy.declaring {
		go func() {
			// drop lists of declared exchanges and queues when connection drops.
			for ; ; time.Sleep(time.Minute) {
				<-cl.conn.NotifyClose()
				cl.opts.lazy.exchangesDeclared.Drop()
				cl.opts.lazy.queueDeclared.Drop()
			}
		}()
	}
	return
}

// Pub publishes v to exchange.
func (c Client) Pub(ctx context.Context, exchangeName string, v interface{}, opts ...ClientConfig) error {
	applyConfigs(&c, opts...)
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
	defer channel.Close()
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

// Sub subscribes to exchange and consume deliveries and converts their Body field to given dataType.
func (c Client) Sub(exchangeName string, dataType interface{}, opts ...ClientConfig) (<-chan Event, chan<- conn.Signal) {
	eventChan := make(chan Event, c.opts.subEventChanBuffer)
	doneCh := make(chan conn.Signal)
	go c.listen(exchangeName, dataType, eventChan, doneCh, opts...)
	return eventChan, doneCh
}

func (c Client) listen(exchangeName string, dataType interface{}, eventChan chan<- Event, doneChan <-chan conn.Signal, opts ...ClientConfig) {
	applyConfigs(&c, opts...)
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
	c.processersPool(queueName, exchangeName, channel, deliveryCh, dataType, eventChan, doneChan)
	go func() {
		select {
		case <-doneChan:
			if channel != nil {
				channel.QueueUnbind(queueName, c.cfgs.queueBind.Key, exchangeName, c.cfgs.queueBind.Args)
				channel.QueueDelete(queueName, c.cfgs.queue.IfUnused, c.cfgs.queue.IfEmpty, c.cfgs.queue.NoWait)
				channel.Close()
			}
			return
		default:
			c.listen(exchangeName, dataType, eventChan, doneChan, opts...)
		}
	}()
}

var (
	NotAllowedPriority            = errors.New("not allowed priority")
	DeliveryChannelWasClosedError = errors.New("delivery channel was closed")
	// Durable or non-auto-delete queues with empty names will survive when all consumers have finished using it, but no one can connect to it back.
	QueueDeclareWarning = errors.New("declaring durable or non-auto-delete queue with empty name")
	// You should use LazyDeclaring option only in Client constructors.
	LazyDeclaringFatal = errors.New("LazyDeclaring not available as option for this method")
)

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
	if c.cfgs.queue.Name == "" && (c.cfgs.queue.Durable || !c.cfgs.queue.AutoDelete) {
		c.opts.log.warn.Log(QueueDeclareWarning)
	}
	c.opts.log.debug.Log(fmt.Errorf("queue(%s) declare", c.cfgs.queue.Name))
	queue, err := c.channelQueueDeclare(channel, c.cfgs.queue)
	if err != nil {
		return nil, "", fmt.Errorf("queue declare err: %v", err)
	}
	c.opts.log.debug.Log(fmt.Errorf("bind queue(%s) to exchange(%s)", queue.Name, exchangeName))
	err = channelQueueBind(channel, queue.Name, exchangeName, c.cfgs.queueBind)
	if err != nil {
		return nil, "", fmt.Errorf("queue bind err: %v", err)
	}
	c.opts.log.debug.Log(fmt.Errorf("consume from queue(%s)", queue.Name))
	ch, err := channelConsume(channel, queue.Name, c.cfgs.consume)
	if err != nil {
		return nil, "", fmt.Errorf("channel consume err: %v", err)
	}
	return ch, queue.Name, nil
}

// processersPool wraps processEvents with WorkerPool pattern.
func (c Client) processersPool(
	queueName, exchangeName string,
	channel *amqp.Channel,
	deliveryCh <-chan amqp.Delivery,
	dataType interface{},
	eventChan chan<- Event,
	doneChan <-chan conn.Signal,
) {
	var wg sync.WaitGroup
	wg.Add(c.opts.handlersAmount)
	for i := 0; i < c.opts.handlersAmount; i++ {
		go func() {
			c.processEvents(queueName, exchangeName, channel, deliveryCh, dataType, eventChan, doneChan)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (c Client) processEvents(
	queueName, exchangeName string,
	channel *amqp.Channel,
	deliveryCh <-chan amqp.Delivery,
	dataType interface{},
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
			c.processEvent(d, dataType, eventChan)
		case <-doneChan:
			if c.opts.processAllDeliveries && processedAll {
				close(eventChan)
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c Client) processEvent(d amqp.Delivery, dataType interface{}, eventChan chan<- Event) {
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
	ev, err := c.handleEvent(d, dataType)
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

func (c Client) checkEvent(d amqp.Delivery) error {
	priorityOk := c.opts.msgOpts.minPriority <= d.Priority && d.Priority <= c.opts.msgOpts.maxPriority
	if !priorityOk {
		return NotAllowedPriority
	}
	return nil
}

func (c Client) handleEvent(d amqp.Delivery, dataType interface{}) (ev Event, err error) {
	ctx := c.opts.context
	for _, before := range c.opts.msgOpts.deliveryBefore {
		ctx = before(ctx, &d)
	}
	ev.Context = ctx

	codec, ok := codecs.Register.Get(d.ContentType)
	if !ok {
		return ev, CodecNotFound
	}
	data := reflect.New(reflect.Indirect(reflect.ValueOf(dataType)).Type()).Interface()
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

type ClientConfig func(*Client)

func applyConfigs(cl *Client, opts ...ClientConfig) {
	for i := range opts {
		opts[i](cl)
	}
}

// Sets amqp.Config which is used to dial to broker.
// Has no effect on NewClientWithConnection, Client.Sub and Client.Pub functions.
func WithConfig(config amqp.Config) ClientConfig {
	return func(client *Client) {
		client.cfgs.conn = &config
	}
}

// WithOptions uses given options to configure and tune Client.
func WithOptions(opts ...Option) ClientConfig {
	return func(client *Client) {
		for i := range opts {
			opts[i](&client.opts)
		}
	}
}

// WithConnOptions sets options that should be used to create new connection.
// Has no effect on NewClientWithConnection, Client.Sub and Client.Pub functions.
func WithConnOptions(opts ...conn.ConnectionOption) ClientConfig {
	return func(client *Client) {
		client.opts.connOpts = append(client.opts.connOpts, opts...)
	}
}

func SetExchangeConfig(cfg ExchangeConfig) ClientConfig {
	return func(client *Client) {
		client.cfgs.exchange = cfg
	}
}

func SetQueueConfig(cfg QueueConfig) ClientConfig {
	return func(client *Client) {
		client.cfgs.queue = cfg
	}
}

func SetQueueBindConfig(cfg QueueBindConfig) ClientConfig {
	return func(client *Client) {
		client.cfgs.queueBind = cfg
	}
}

func SetConsumeConfig(cfg ConsumeConfig) ClientConfig {
	return func(client *Client) {
		client.cfgs.consume = cfg
	}
}

func SetPublishConfig(cfg PublishConfig) ClientConfig {
	return func(client *Client) {
		client.cfgs.publish = cfg
	}
}
