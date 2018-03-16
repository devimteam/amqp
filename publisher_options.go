package amqp

import (
	"time"

	"github.com/devimteam/amqp/logger"
)

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

// WaitConnection tells client to wait connection before Subscription or Pub executing.
func SubWaitConnection(should bool, timeout time.Duration) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.wait.flag = should
		if timeout != 0 {
			subscriber.opts.wait.timeout = timeout
		}
	}
}

// PublishBefore adds functions, that should be called before publishing message to broker.
func PublishBefore(before ...PublishingBefore) Option {
	return func(options *options) {
		for i := range before {
			options.msgOpts.pubBefore = append(options.msgOpts.pubBefore, before[i])
		}
	}
}

// WarnLogger option sets logger, which logs warning messages.
func WarnLogger(lg logger.Logger) Option {
	return func(options *options) {
		options.log.warn = lg
	}
}

func SubWithObserverOptions(opts ...ObserverOption) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.observerOpts = append(subscriber.opts.observerOpts, opts...)
	}
}
