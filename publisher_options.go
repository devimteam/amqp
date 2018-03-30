package amqp

import (
	"time"

	"github.com/devimteam/amqp/logger"
)

type PublisherOption func(*Publisher)

type publisherOptions struct {
	before             []PublishingBefore
	defaultContentType string
	wait               struct {
		flag    bool
		timeout time.Duration
	}
	log          logger.Logger
	observerOpts []ObserverOption
	workers      int
}

func defaultPubOptions() publisherOptions {
	opts := publisherOptions{}
	opts.wait.flag = true
	opts.wait.timeout = defaultWaitDeadline
	opts.log = logger.NoopLogger
	opts.defaultContentType = defaultContentType
	opts.workers = defaultWorkers
	return opts
}

// WaitConnection tells client to wait connection before Subscription or Pub executing.
func PublisherWaitConnection(should bool, timeout time.Duration) PublisherOption {
	return func(subscriber *Publisher) {
		subscriber.opts.wait.flag = should
		if timeout != 0 {
			subscriber.opts.wait.timeout = timeout
		}
	}
}

// PublishBefore adds functions, that should be called before publishing message to broker.
func PublisherBefore(before ...PublishingBefore) PublisherOption {
	return func(publisher *Publisher) {
		publisher.opts.before = append(publisher.opts.before, before...)
	}
}

// WarnLogger option sets logger, which logs warning messages.
func PublisherLogger(lg logger.Logger) PublisherOption {
	return func(publisher *Publisher) {
		publisher.opts.log = lg
	}
}

func PublisherWithObserverOptions(opts ...ObserverOption) PublisherOption {
	return func(publisher *Publisher) {
		publisher.opts.observerOpts = append(publisher.opts.observerOpts, opts...)
	}
}

// HandlersAmount sets the amount of handle processes, which receive deliveries from one channel.
// For n > 1 client does not guarantee the order of events.
func PublisherHandlersAmount(n int) PublisherOption {
	return func(publisher *Publisher) {
		if n > 0 {
			publisher.opts.workers = n
		}
	}
}

func PublisherContentType(t string) PublisherOption {
	return func(publisher *Publisher) {
		publisher.opts.defaultContentType = t
	}
}
