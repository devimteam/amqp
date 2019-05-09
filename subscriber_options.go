package amqp

import (
	"time"

	"github.com/devimteam/amqp/logger"
	"github.com/devimteam/amqp/metrics"
)

type subscriberOptions struct {
	channelBuffer int
	wait          struct {
		flag    bool
		timeout time.Duration
	}
	workers       int
	processAll    bool
	log           logger.Logger
	msgOpts       subMessageOptions
	errorBefore   []ErrorBefore
	observerOpts  []ObserverOption
	counterMetric metrics.Counter
}

func defaultSubOptions() subscriberOptions {
	opts := subscriberOptions{}
	opts.processAll = false
	opts.wait.flag = true
	opts.wait.timeout = defaultWaitDeadline
	opts.channelBuffer = defaultEventBuffer
	opts.msgOpts.minPriority = MinMessagePriority
	opts.msgOpts.maxPriority = MaxMessagePriority
	opts.msgOpts.defaultContentType = defaultContentType
	opts.workers = defaultWorkers
	opts.log = logger.NoopLogger
	opts.counterMetric = metrics.NoopCounter
	return opts
}

// WaitConnection tells client to wait connection before Subscription or Pub executing.
func SubscriberWaitConnection(should bool, timeout time.Duration) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.wait.flag = should
		if timeout != 0 {
			subscriber.opts.wait.timeout = timeout
		}
	}
}

// EventChanBuffer sets the buffer of event channel for Subscription method.
func SubscriberBufferSize(a int) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.channelBuffer = a
	}
}

// AllowedPriority rejects messages, which not in range.
func SubscriberAllowedPriority(from, to uint8) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.msgOpts.minPriority = from
		subscriber.opts.msgOpts.maxPriority = to
	}
}

// SubscriberLogger option sets logger, which logs error messages.
func SubscriberLogger(lg logger.Logger) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.log = lg
	}
}

// DeliverBefore adds functions, that should be called before sending Event to channel.
func SubscriberDeliverBefore(before ...DeliveryBefore) SubscriberOption {
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
// JSON is used by default.
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

func SubProcessedMetric(counter metrics.Counter) SubscriberOption {
	return func(subscriber *Subscriber) {
		subscriber.opts.counterMetric = counter
	}
}
