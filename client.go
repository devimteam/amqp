package amqp

import (
	"context"
	"errors"

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

/*
// Client is a main object, that controls all processes behind Pub and Subscription calls.
type Client struct {
	cfgs     configs
	opts     options
	conn     *conn.Connection
	observer *observer
}

// NewClientWithConnection is used when you want to pass connection directly. Otherwise, please use NewClient.
func NewClientWithConnection(conn *conn.Connection, cfgs ...ClientConfig) (cl Client, err error) {
	cl.constructorBefore(cfgs...)
	cl.conn = conn
	err = cl.constructorAfter()
	return
}

// NewClient is a common way to create a new Client.
func NewClient(url string, cfgs ...ClientConfig) (cl Client, err error) {
	cl.constructorBefore(cfgs...)
	if cl.cfgs.conn != nil {
		cl.conn, _ = conn.DialConfig(url, *cl.cfgs.conn, cl.opts.connOpts...)
	} else {
		cl.conn, _ = conn.Dial(url, cl.opts.connOpts...)
	}
	err = cl.constructorAfter()
	return
}

func (c *Client) constructorBefore(cfgs ...ClientConfig) {
	c.opts = defaultOptions()
	c.cfgs = newConfigs()
	applyConfigs(c, cfgs...)
}

func (c *Client) constructorAfter() error {
	c.observer = newObserver(c.conn, c.opts.observerOpts...)
	return nil
}

// Pub publishes v to exchange.
func (c Client) Pub(ctx context.Context, exchangeName string, v interface{}, opts ...ClientConfig) error {
	applyConfigs(&c, opts...)
	if c.opts.wait.flag {
		err := c.conn.NotifyConnected(c.opts.wait.timeout)
		if err != nil {
			return err
		}
	}
	channel := c.observer.channel()
	defer c.observer.release(channel)
	return c.publish(channel, ctx, exchangeName, v, &c.cfgs.exchange)
}

// Publishing provides channel for Pub calls. It uses own amqp channel to send messages.
func (c Client) Publishing(ctx context.Context, exchangeName string, cfg ExchangeConfig) (chan<- interface{}, error) {
	ch := make(chan interface{}, c.opts.pubEventChanBuffer)
	go func() {
		channel := c.observer.channel()
		defer c.observer.release(channel)
		for v := range ch {
			for ; ; time.Sleep(time.Millisecond * 100) {
				err := c.publish(channel, ctx, exchangeName, v, &cfg)
				if err != nil {
					c.opts.log.warn.Log(err)
				} else {
					break
				}
			}
		}
	}()
	return ch, nil
}

func (c *Client) publish(channel *Channel, ctx context.Context, exchangeName string, v interface{}, cfg *ExchangeConfig) error {
	err := channel.ExchangeDeclare(exchangeName, cfg)
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
	err = channel.Publish(exchangeName, msg, c.cfgs.publish)
	if err != nil {
		return WrapError("publish", err)
	}
	return nil
}

// Subscription subscribes to exchange, consume deliveries and converts their Body field to given dataType.
func (c Client) Subscription(exchangeName string, dataType interface{}, opts ...ClientConfig) (<-chan Event, chan<- conn.Signal) {
	eventChan := make(chan Event, c.opts.subEventChanBuffer)
	doneCh := make(chan conn.Signal)
	channel := c.observer.channel()
	go c.listen(channel, exchangeName, dataType, eventChan, doneCh, opts...)
	return eventChan, doneCh
}

func (c Client) listen(channel *Channel, exchangeName string, dataType interface{}, eventChan chan<- Event, doneChan <-chan conn.Signal, opts ...ClientConfig) {
	applyConfigs(&c, opts...)
	for {
		select {
		case <-doneChan:
			if channel != nil {
				c.observer.release(channel)
			}
			return
		default:
			if c.opts.wait.flag {
				err := c.conn.NotifyConnected(c.opts.wait.timeout)
				if err != nil {
					c.opts.log.error.Log(err)
				}
			}
			deliveryCh, queueName, err := c.prepareDeliveryChan(channel, exchangeName)
			if err != nil {
				c.opts.log.error.Log(err)
				continue
			}
			c.processersPool(queueName, exchangeName, deliveryCh, dataType, eventChan, doneChan)
		}
	}
}

func (c Client) prepareDeliveryChan(
	channel *Channel,
	exchangeName string,
) (<-chan amqp.Delivery, string, error) {
	c.opts.log.debug.Log(fmt.Errorf("prepare delivery chan for exchange %s", exchangeName))
	c.opts.log.debug.Log(fmt.Errorf("exchange(%s) declare", exchangeName))
	err := channel.ExchangeDeclare(exchangeName, &c.cfgs.exchange)
	if err != nil {
		return nil, "", WrapError("exchange declare err", err)
	}
	if c.cfgs.queue.Name == "" && (c.cfgs.queue.Durable || !c.cfgs.queue.AutoDelete) {
		c.opts.log.warn.Log(QueueDeclareWarning)
	}
	c.opts.log.debug.Log(fmt.Errorf("queue(%s) declare", c.cfgs.queue.Name))
	queue, err := channel.QueueDeclare(c.cfgs.queue)
	if err != nil {
		return nil, "", WrapError("queue declare err", err)
	}
	c.opts.log.debug.Log(fmt.Errorf("bind queue(%s) to exchange(%s)", queue.Name, exchangeName))
	err = channel.QueueBind(queue.Name, exchangeName, c.cfgs.queueBind, c.cfgs.queue.Name == "")
	if err != nil {
		return nil, "", WrapError("queue bind err", err)
	}
	c.opts.log.debug.Log(fmt.Errorf("consume from queue(%s)", queue.Name))
	ch, err := channel.Consume(queue.Name, c.cfgs.consume)
	if err != nil {
		return nil, "", WrapError("channel consume err", err)
	}
	return ch, queue.Name, nil
}

// processersPool wraps processEvents with WorkerPool pattern.
func (c Client) processersPool(
	queueName, exchangeName string,
	deliveryCh <-chan amqp.Delivery,
	dataType interface{},
	eventChan chan<- Event,
	doneChan <-chan conn.Signal,
) {
	var wg sync.WaitGroup
	wg.Add(c.opts.handlersAmount)
	for i := 0; i < c.opts.handlersAmount; i++ {
		go func() {
			c.processEvents(queueName, exchangeName, deliveryCh, dataType, eventChan, doneChan)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (c Client) processEvents(
	queueName, exchangeName string,
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
			c.opts.log.debug.Log(WrapError("process delivery", d.MessageId))
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
	ev.Delivery = d

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
*/
/*
// WithConfig sets amqp.Config which is used to dial to broker.
// Has no effect on NewClientWithConnection, Client.Subscription and Client.Pub functions.
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
// Has no effect on NewClientWithConnection, Client.Subscription and Client.Pub functions.
func WithConnOptions(opts ...conn.ConnectionOption) ClientConfig {
	return func(client *Client) {
		client.opts.connOpts = append(client.opts.connOpts, opts...)
	}
}

func WithObserverOptions(opts ...ObserverOption) ClientConfig {
	return func(client *Client) {
		client.opts.observerOpts = append(client.opts.observerOpts, opts...)
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
*/
