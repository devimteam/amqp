package amqp

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/devimteam/amqp/codecs"
	"github.com/devimteam/amqp/conn"
	"github.com/streadway/amqp"
)

type (
	Subscriber struct {
		conn     *conn.Connection
		observer *observer
		opts     subscriberOptions
	}
	SubscriberOption func(*Subscriber)
)

func newSubscriber(ctx context.Context, conn *conn.Connection, opts ...SubscriberOption) *Subscriber {
	s := Subscriber{}
	s.opts = defaultSubOptions()
	for _, opt := range opts {
		opt(&s)
	}
	s.conn = conn
	s.observer = newObserver(ctx, s.conn, s.opts.observerOpts...)
	return &s
}

func (s Subscriber) Subscribe(ctx context.Context, exchangeName, queueName string, dataType interface{}, cfg Consumer) <-chan Event {
	eventChan := make(chan Event, s.opts.channelBuffer)
	channel := s.observer.channel()
	go s.listen(ctx, channel, exchangeName, queueName, dataType, eventChan, cfg)
	return eventChan
}

func (s Subscriber) SubscribeToQueue(ctx context.Context, queueName string, dataType interface{}, cfg Consumer) <-chan Event {
	return s.Subscribe(ctx, "", queueName, dataType, cfg)
}

func (s Subscriber) SubscribeToExchange(ctx context.Context, exchangeName string, dataType interface{}, cfg Consumer) <-chan Event {
	return s.Subscribe(ctx, exchangeName, "", dataType, cfg)
}

func (s Subscriber) listen(ctx context.Context, channel *Channel, exchangeName, queueName string, dataType interface{}, eventChan chan<- Event, cfg Consumer) {
	for {
		select {
		case <-ctx.Done():
			if channel != nil {
				s.observer.release(channel)
			}
			return
		default:
			if s.opts.wait.flag {
				err := s.conn.NotifyConnected(s.opts.wait.timeout)
				if err != nil {
					s.opts.log.error.Log(err)
				}
			}
			deliveryCh, err := s.prepareDeliveryChan(channel, exchangeName, queueName, cfg)
			if err != nil {
				s.opts.log.error.Log(err)
				continue
			}
			s.workersPool(ctx, queueName, deliveryCh, dataType, eventChan)
		}
	}
}

func (s Subscriber) prepareDeliveryChan(
	channel *Channel,
	queueName, exchangeName string,
	cfg Consumer,
) (<-chan amqp.Delivery, error) {
	if queueName == "" {
		queue, err := channel.declareQueue(Queue{
			AutoDelete: true,
			Durable:    false,
		})
		if err != nil {
			return nil, WrapError("declare queue", err)
		}
		err = channel.bind(Binding{
			Queue:    queue.Name,
			Exchange: exchangeName,
			Key:      "",
			Args:     nil,
			NoWait:   false,
		})
		if err != nil {
			return nil, WrapError("bind", queue.Name, "to", exchangeName, err)
		}
	}
	ch, err := channel.consume(queueName, cfg)
	if err != nil {
		return nil, WrapError("channel consume err", err)
	}
	return ch, nil
}

// workersPool wraps processEvents with WorkerPool pattern.
func (s Subscriber) workersPool(
	ctx context.Context,
	queueName string,
	deliveryCh <-chan amqp.Delivery,
	dataType interface{},
	eventChan chan<- Event,
) {
	var wg sync.WaitGroup
	wg.Add(s.opts.workers)
	for i := 0; i < s.opts.workers; i++ {
		go func() {
			s.processEvents(ctx, queueName, deliveryCh, dataType, eventChan)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s Subscriber) processEvents(
	ctx context.Context,
	queueName string,
	deliveryCh <-chan amqp.Delivery,
	dataType interface{},
	eventChan chan<- Event,
) {
	processedAll := false
	for {
		select {
		case d, ok := <-deliveryCh:
			if !ok {
				processedAll = true
				return
			}
			s.processEvent(d, dataType, eventChan)
		case <-ctx.Done():
			if s.opts.processAll && processedAll {
				close(eventChan)
				return
			}
		}
	}
}

func (s Subscriber) processEvent(d amqp.Delivery, dataType interface{}, eventChan chan<- Event) {
	err := s.checkEvent(d)
	if err != nil {
		err = s.errorBefore(d, err)
		s.opts.log.warn.Log(err)
		e := d.Nack(false, true)
		if e != nil {
			s.opts.log.error.Log(fmt.Errorf("nack delivery: %v because of %v", e, err))
		}
		return
	}
	ev, err := s.handleEvent(d, dataType)
	if err != nil {
		err = s.errorBefore(d, err)
		s.opts.log.warn.Log(err)
		e := d.Nack(false, true)
		if e != nil {
			s.opts.log.error.Log(fmt.Errorf("nack delivery: %v because of %v", e, err))
		}
		return
	}
	eventChan <- ev
}

func (s Subscriber) checkEvent(d amqp.Delivery) error {
	priorityOk := s.opts.msgOpts.minPriority <= d.Priority && d.Priority <= s.opts.msgOpts.maxPriority
	if !priorityOk {
		return NotAllowedPriority
	}
	return nil
}

func (s Subscriber) handleEvent(d amqp.Delivery, dataType interface{}) (ev Event, err error) {
	ctx := s.opts.context
	for _, before := range s.opts.msgOpts.deliveryBefore {
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
func (s Subscriber) errorBefore(d amqp.Delivery, err error) error {
	for _, before := range s.opts.errorBefore {
		err = before(d, err)
	}
	return err
}
