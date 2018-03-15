package amqp

import (
	"context"
	"sync"
	"time"

	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
)

type Publisher struct {
	observer       *observer
	conn           *conn.Connection
	opts           publisherOptions
	defaultPublish Publish
}

type PublisherOption func(*Publisher)

type publisherOptions struct {
	msgOpts pubMessageOptions
	wait    struct {
		flag    bool
		timeout time.Duration
	}
	log struct {
		warn logger.Logger
	}
	observerOpts []ObserverOption
	workers      int
}

func defaultPubOptions() publisherOptions {
	opts := publisherOptions{}
	opts.wait.flag = true
	opts.wait.timeout = defaultWaitDeadline
	opts.msgOpts.idBuilder = noopMessageIdBuilder
	opts.msgOpts.typer = noopTyper
	opts.log.warn = logger.NoopLogger
	opts.msgOpts.defaultContentType = "application/json"
	return opts
}

func newPublisher(conn *conn.Connection, opts ...PublisherOption) *Publisher {
	p := Publisher{}
	for _, opt := range opts {
		opt(&p)
	}
	p.conn = conn
	p.observer = newObserver(p.conn, p.opts.observerOpts...)
	return &p
}

func (p *Publisher) Publish(ctx context.Context, exchangeName string, obj interface{}, pub Publish) error {
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

func (p *Publisher) PublishChannel(ctx context.Context, exchangeName string, pub Publish) chan<- interface{} {
	channel := make(chan interface{})
	go func() {
		amqpChan := p.observer.channel()
		defer p.observer.release(amqpChan)
		p.workerPool(amqpChan, channel, ctx, exchangeName, pub)
	}()
	return channel
}

func (p *Publisher) workerPool(amqpChan *Channel, channel <-chan interface{}, ctx context.Context, exchangeName string, pub Publish) {
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

func (p *Publisher) worker(amqpChan *Channel, channel <-chan interface{}, ctx context.Context, exchangeName string, pub Publish) {
	for obj := range channel {
		err := p.publish(amqpChan, ctx, exchangeName, obj, pub)
		if err != nil {
			p.opts.log.warn.Log(err)
		}
	}
}

func (p *Publisher) publish(channel *Channel, ctx context.Context, exchangeName string, v interface{}, publish Publish) error {
	msg, err := constructPublishing(v, publish.Priority, &p.opts.msgOpts)
	if err != nil {
		return err
	}
	for _, before := range p.opts.msgOpts.pubBefore {
		before(ctx, &msg)
	}
	err = channel.publish(exchangeName, msg, publish)
	if err != nil {
		return WrapError("publish", err)
	}
	return nil
}
