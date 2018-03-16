package amqp

import (
	"context"
	"time"

	"github.com/devimteam/amqp/logger"
)

type subscriberOptions struct {
	channelBuffer int
	wait          struct {
		flag    bool
		timeout time.Duration
	}
	workers    int
	processAll bool
	log        struct {
		warn  logger.Logger
		error logger.Logger
	}
	msgOpts      subMessageOptions
	context      context.Context
	errorBefore  []ErrorBefore
	observerOpts []ObserverOption
}

func defaultSubOptions() subscriberOptions {
	opts := subscriberOptions{}
	opts.processAll = false
	opts.context = context.Background()
	opts.wait.flag = true
	opts.wait.timeout = defaultWaitDeadline
	opts.channelBuffer = defaultEventBuffer
	opts.msgOpts.minPriority = MinMessagePriority
	opts.msgOpts.maxPriority = MaxMessagePriority
	opts.msgOpts.defaultContentType = "application/json"
	opts.workers = defaultWorkers
	opts.log.warn = logger.NoopLogger
	opts.log.error = logger.NoopLogger
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

// EventChanBuffer sets the buffer of event channel for Subscription method.
func SubChannelBuffer(a int) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.channelBuffer = a
	}
}

// Context sets root context of Subscription method for each event.
// context.Background by default.
func SubContext(ctx context.Context) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.context = ctx
	}
}

// AllowedPriority rejects messages, which not in range.
func SubAllowedPriority(from, to uint8) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.msgOpts.minPriority = from
		subscriber.opts.msgOpts.maxPriority = to
	}
}

// ErrorLogger option sets logger, which logs error messages.
func SubErrorLogger(lg logger.Logger) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.log.error = lg
	}
}

// WarnLogger option sets logger, which logs warning messages.
func SubWarnLogger(lg logger.Logger) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.log.warn = lg
	}
}

// DeliverBefore adds functions, that should be called before sending Event to channel.
func SubDeliverBefore(before ...DeliveryBefore) SubscriberOption {
	return func(subscriber *Subscriber) {
		for i := range before {
			subscriber.opts.msgOpts.deliveryBefore = append(subscriber.opts.msgOpts.deliveryBefore, before[i])
		}
	}
}

// Add this option with true value that allows you to handle all deliveries from current channel, even if the Done was sent.
func SubProcessAllDeliveries(v bool) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.processAll = v
	}
}

// HandlersAmount sets the amount of handle processes, which receive deliveries from one channel.
// For n > 1 client does not guarantee the order of events.
func SubHandlersAmount(n int) SubscriberOption {
	return func(subscriber *Subscriber) {
		if n > 0 {
			subscriber.opts.workers = n
		}
	}
}

// SetDefaultContentType sets content type which codec should be used if ContentType field of message is empty.
func SubSetDefaultContentType(t string) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.msgOpts.defaultContentType = t
	}
}

func SubWithObserverOptions(opts ...ObserverOption) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.observerOpts = append(subscriber.opts.observerOpts, opts...)
	}
}
