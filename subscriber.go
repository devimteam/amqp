package amqp

import (
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

func newSubscriber(conn *conn.Connection, opts ...SubscriberOption) *Subscriber {
	s := Subscriber{}
	s.opts = defaultSubOptions()
	for _, opt := range opts {
		opt(&s)
	}
	s.conn = conn
	s.observer = newObserver(s.conn, s.opts.observerOpts...)
	return &s
}

func (s *Subscriber) Subscribe(exchangeName, queueName string, dataType interface{}, cfg ConsumeConfig) (<-chan Event, chan<- conn.Signal) {
	eventChan := make(chan Event, s.opts.channelBuffer)
	doneCh := make(chan conn.Signal)
	channel := s.observer.channel()
	go s.listen(channel, exchangeName, queueName, dataType, eventChan, doneCh, cfg)
	return eventChan, doneCh
}

func (s *Subscriber) SubscribeToQueue(queueName string, dataType interface{}, cfg ConsumeConfig) (<-chan Event, chan<- conn.Signal) {
	return s.Subscribe("", queueName, dataType, cfg)
}

func (s *Subscriber) SubscribeToExchange(exchangeName string, dataType interface{}, cfg ConsumeConfig) (<-chan Event, chan<- conn.Signal) {
	return s.Subscribe(exchangeName, "", dataType, cfg)
}

func (s *Subscriber) listen(channel *Channel, exchangeName, queueName string, dataType interface{}, eventChan chan<- Event, doneChan <-chan conn.Signal, cfg ConsumeConfig) {
	for {
		select {
		case <-doneChan:
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
			s.workersPool(queueName, deliveryCh, dataType, eventChan, doneChan)
		}
	}
}

func (s *Subscriber) prepareDeliveryChan(
	channel *Channel,
	queueName, exchangeName string,
	cfg ConsumeConfig,
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
	ch, err := channel.Consume(queueName, cfg)
	if err != nil {
		return nil, WrapError("channel consume err", err)
	}
	return ch, nil
}

// workersPool wraps processEvents with WorkerPool pattern.
func (s *Subscriber) workersPool(
	queueName string,
	deliveryCh <-chan amqp.Delivery,
	dataType interface{},
	eventChan chan<- Event,
	doneChan <-chan conn.Signal,
) {
	var wg sync.WaitGroup
	wg.Add(s.opts.workers)
	for i := 0; i < s.opts.workers; i++ {
		go func() {
			s.processEvents(queueName, deliveryCh, dataType, eventChan, doneChan)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *Subscriber) processEvents(
	queueName string,
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
				return
			}
			s.processEvent(d, dataType, eventChan)
		case <-doneChan:
			if s.opts.processAll && processedAll {
				close(eventChan)
				return
			}
		}
	}
}

func (s *Subscriber) processEvent(d amqp.Delivery, dataType interface{}, eventChan chan<- Event) {
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

func (s *Subscriber) checkEvent(d amqp.Delivery) error {
	priorityOk := s.opts.msgOpts.minPriority <= d.Priority && d.Priority <= s.opts.msgOpts.maxPriority
	if !priorityOk {
		return NotAllowedPriority
	}
	return nil
}

func (s *Subscriber) handleEvent(d amqp.Delivery, dataType interface{}) (ev Event, err error) {
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
func (s *Subscriber) errorBefore(d amqp.Delivery, err error) error {
	for _, before := range s.opts.errorBefore {
		err = before(d, err)
	}
	return err
}
