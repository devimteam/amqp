package amqp

import (
	"context"
	"fmt"
	"time"

	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

const (
	MaxMessagePriority = 9
	MinMessagePriority = 0
)

const (
	defaultWaitDeadline  = time.Second * 5
	defaultEventBuffer   = 1
	defaultHandlerAmount = 1
)

type (
	Option func(*options)
	// Options is a struct with almost all possible options of Client.
	options struct {
		wait struct {
			flag    bool
			timeout time.Duration
		}
		timeout struct {
			base time.Duration
			cap  int
		}
		subEventChanBuffer int
		pubEventChanBuffer int
		log                struct {
			debug logger.Logger
			info  logger.Logger
			warn  logger.Logger
			error logger.Logger
		}
		context              context.Context
		msgOpts              messageOptions
		processAllDeliveries bool
		handlersAmount       int
		errorBefore          []ErrorBefore
		connOpts             []conn.ConnectionOption
		observerOpts         []ObserverOption
	}

	// Function, that should return new message Id.
	MessageIdBuilder func() string
	// Function, that should return string representation of type's value.
	Typer func(value interface{}) string
	// Function, that changes message before publishing.
	PublishingBefore func(context.Context, *amqp.Publishing)
	// Function, that changes message before delivering.
	DeliveryBefore func(context.Context, *amqp.Delivery) context.Context
	// Function, that changes error, which caused on incorrect handling.
	// Common use-case: debugging.
	ErrorBefore func(amqp.Delivery, error) error
)

func defaultOptions() options {
	opts := options{}
	opts.context = context.Background()
	opts.wait.timeout = defaultWaitDeadline
	opts.subEventChanBuffer = defaultEventBuffer
	opts.msgOpts.idBuilder = noopMessageIdBuilder
	opts.msgOpts.minPriority = MinMessagePriority
	opts.msgOpts.maxPriority = MaxMessagePriority
	opts.msgOpts.typer = noopTyper
	opts.handlersAmount = defaultHandlerAmount
	opts.log.debug = logger.NoopLogger
	opts.log.info = logger.NoopLogger
	opts.log.warn = logger.NoopLogger
	opts.log.error = logger.NoopLogger
	opts.msgOpts.defaultContentType = "application/json"
	return opts
}

// WaitConnection tells client to wait connection before Subscription or Pub executing.
func WaitConnection(should bool, timeout time.Duration) Option {
	return func(options *options) {
		options.wait.flag = should
		if timeout != 0 {
			options.wait.timeout = timeout
		}
	}
}

// EventChanBuffer sets the buffer of event channel for Subscription method.
func EventChanBuffer(a int) Option {
	return func(options *options) {
		options.subEventChanBuffer = a
	}
}

// PublisherChanBuffer sets the buffer of event channel for Publishing method.
func PublisherChanBuffer(a int) Option {
	return func(options *options) {
		options.pubEventChanBuffer = a
	}
}

// Context sets root context of Subscription method for each event.
// context.Background by default.
func Context(ctx context.Context) Option {
	return func(options *options) {
		options.context = ctx
	}
}

// SetMessageIdBuilder sets function, that executes on every Pub call and result can be interpreted as message Id.
func SetMessageIdBuilder(builder MessageIdBuilder) Option {
	return func(options *options) {
		options.msgOpts.idBuilder = builder
	}
}

// AllowedPriority rejects messages, which not in range.
func AllowedPriority(from, to uint8) Option {
	return func(options *options) {
		options.msgOpts.minPriority = from
		options.msgOpts.maxPriority = to
	}
}

// ApplicationId adds this Id to each message, which created on Pub call.
func ApplicationId(id string) Option {
	return func(options *options) {
		options.msgOpts.applicationId = id
	}
}

// ApplicationId adds this Id to each message, which created on Pub call.
func UserId(id string) Option {
	return func(options *options) {
		options.msgOpts.userId = id
	}
}

// InfoLogger option sets logger, which logs info messages.
func InfoLogger(lg logger.Logger) Option {
	return func(options *options) {
		options.log.info = lg
	}
}

// DebugLogger option sets logger, which logs debug messages.
func DebugLogger(lg logger.Logger) Option {
	return func(options *options) {
		options.log.debug = lg
	}
}

// ErrorLogger option sets logger, which logs error messages.
func ErrorLogger(lg logger.Logger) Option {
	return func(options *options) {
		options.log.error = lg
	}
}

// WarnLogger option sets logger, which logs warning messages.
func WarnLogger(lg logger.Logger) Option {
	return func(options *options) {
		options.log.warn = lg
	}
}

// AllLoggers option is a shortcut for call each <*>Logger with the same logger.
func AllLoggers(lg logger.Logger) Option {
	return func(options *options) {
		options.log.info = lg
		options.log.debug = lg
		options.log.error = lg
		options.log.warn = lg
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

// DeliverBefore adds functions, that should be called before sending Event to channel.
func DeliverBefore(before ...DeliveryBefore) Option {
	return func(options *options) {
		for i := range before {
			options.msgOpts.deliveryBefore = append(options.msgOpts.deliveryBefore, before[i])
		}
	}
}

// Add this option with true value that allows you to handle all deliveries from current channel, even if the Done was sent.
func ProcessAllDeliveries(v bool) Option {
	return func(options *options) {
		options.processAllDeliveries = v
	}
}

// HandlersAmount sets the amount of handle processes, which receive deliveries from one channel.
// For n > 1 client does not guarantee the order of events.
func HandlersAmount(n int) Option {
	return func(options *options) {
		if n > 0 {
			options.handlersAmount = n
		}
	}
}

// SetDefaultContentType sets content type which codec should be used if ContentType field of message is empty.
func SetDefaultContentType(t string) Option {
	return func(options *options) {
		options.msgOpts.defaultContentType = t
	}
}

var noopMessageIdBuilder = func() string {
	return ""
}

var noopTyper = func(_ interface{}) string {
	return ""
}

// CommonTyper prints go-style type of value.
func CommonTyper(v interface{}) string {
	return fmt.Sprintf("%T", v)
}

// CommonMessageIdBuilder builds new UUID as message Id.
func CommonMessageIdBuilder() string {
	return uuid.NewV4().String()
}
