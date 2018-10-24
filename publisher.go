package amqp

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/devimteam/amqp/conn"
)

type Publisher struct {
	observer       *observer
	conn           *conn.Connection
	opts           publisherOptions
	defaultPublish Publish
}

func newPublisher(ctx context.Context, conn *conn.Connection, opts ...PublisherOption) *Publisher {
	p := Publisher{}
	p.opts = defaultPubOptions()
	for _, opt := range opts {
		opt(&p)
	}
	p.conn = conn
	p.observer = newObserver(ctx, p.conn, p.opts.observerOpts...)
	return &p
}

func (p Publisher) Publish(ctx context.Context, exchangeName string, obj interface{}, pub Publish) error {
	channel := p.observer.channel()
	if p.opts.wait.flag {
		err := p.conn.NotifyConnected(p.opts.wait.timeout)
		if err != nil {
			return err
		}
	}
	defer p.observer.release(channel)
	return p.publish(channel, ctx, exchangeName, obj, pub)
}

func (p Publisher) PublishChannel(ctx context.Context, exchangeName string, pub Publish) chan<- interface{} {
	channel := make(chan interface{})
	go func() {
		amqpChan := p.observer.channel()
		defer p.observer.release(amqpChan)
		p.workerPool(amqpChan, channel, ctx, exchangeName, pub)
	}()
	return channel
}

func (p Publisher) workerPool(amqpChan *Channel, channel <-chan interface{}, ctx context.Context, exchangeName string, pub Publish) {
	var wg sync.WaitGroup
	wg.Add(p.opts.workers)
	for i := 0; i < p.opts.workers; i++ {
		go func() {
			p.worker(amqpChan, channel, ctx, exchangeName, pub)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (p Publisher) worker(amqpChan *Channel, channel <-chan interface{}, ctx context.Context, exchangeName string, pub Publish) {
	for obj := range channel {
		if err := p.publish(amqpChan, ctx, exchangeName, obj, pub); err != nil {
			_ = p.opts.log.Log(err)
		}
	}
}

func (p Publisher) publish(channel *Channel, ctx context.Context, exchangeName string, v interface{}, publish Publish) error {
	msg, err := constructPublishing(v, publish.Priority, p.opts.defaultContentType)
	if err != nil {
		return err
	}
	for _, before := range p.opts.before {
		before(ctx, &msg)
	}
	if err = channel.publish(exchangeName, msg, publish); err != nil {
		return errors.Wrap(err, "publish")
	}
	return nil
}
