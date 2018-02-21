package amqp

import (
	"context"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type options struct {
	waitConnection         bool
	waitConnectionDeadline time.Duration
	timeoutBase            time.Duration
	timeoutCap             int
	subEventChanBuffer     int
	connectionLogger       Logger
	eventLogger            Logger
	context                context.Context
	msgOpts                messageOptions
	processAllDeliveries   bool
	handlersAmount         int
}

type MessageIdBuilder func() string
type Typer func(value interface{}) string // function, that should return string representation of type's value
type PublishingBefore func(context.Context, *amqp.Publishing)
type DeliveryBefore func(context.Context, *amqp.Delivery) context.Context

const (
	MaxMessagePriority = 9
	MinMessagePriority = 0
)

const (
	defaultWaitDeadline  = time.Second * 5
	defaultEventBuffer   = 1
	defaultHandlerAmount = 1
)

func defaultOptions() options {
	opts := options{}
	opts.context = context.Background()
	opts.waitConnectionDeadline = defaultWaitDeadline
	opts.subEventChanBuffer = defaultEventBuffer
	opts.msgOpts.idBuilder = noopMessageIdBuilder
	opts.msgOpts.minPriority = MinMessagePriority
	opts.msgOpts.maxPriority = MaxMessagePriority
	opts.msgOpts.typer = noopTyper
	opts.handlersAmount = defaultHandlerAmount
	return opts
}

type Option func(*options)

// Timeout sets delays for connection between attempts.
// Has no effect on NewClient function.
func Timeout(base time.Duration, cap int) Option {
	return func(options *options) {
		options.timeoutBase = base
		options.timeoutCap = cap
	}
}

// WaitConnection tells client to wait connection before Sub or Pub executing.
func WaitConnection(should bool, deadline time.Duration) Option {
	return func(options *options) {
		options.waitConnection = should
		if deadline != 0 {
			options.waitConnectionDeadline = deadline
		}
	}
}

// EventChanBuffer sets the buffer of event channel for Sub method.
func EventChanBuffer(a int) Option {
	return func(options *options) {
		options.subEventChanBuffer = a
	}
}

// Context sets root context of Sub method for each event.
// context.Background by default.
func Context(ctx context.Context) Option {
	return func(options *options) {
		options.context = ctx
	}
}

func SetMessageIdBuilder(builder MessageIdBuilder) Option {
	return func(options *options) {
		options.msgOpts.idBuilder = builder
	}
}

func AllowedPriority(from, to uint8) Option {
	return func(options *options) {
		options.msgOpts.minPriority = from
		options.msgOpts.maxPriority = to
	}
}

func ApplicationId(id string) Option {
	return func(options *options) {
		options.msgOpts.applicationId = id
	}
}

func UserId(id string) Option {
	return func(options *options) {
		options.msgOpts.userId = id
	}
}

// EventLogger option sets logger, which logs connection events.
// Has no effect on NewClient function.
func ConnectionLogger(lg Logger) Option {
	return func(options *options) {
		options.connectionLogger = lg
	}
}

// EventLogger option sets logger, which logs events of Sub method.
func EventLogger(lg Logger) Option {
	return func(options *options) {
		options.eventLogger = lg
	}
}

func PublishBefore(before ...PublishingBefore) Option {
	return func(options *options) {
		for i := range before {
			options.msgOpts.pubBefore = append(options.msgOpts.pubBefore, before[i])
		}
	}
}

func DeliverBefore(before ...DeliveryBefore) Option {
	return func(options *options) {
		for i := range before {
			options.msgOpts.delBefore = append(options.msgOpts.delBefore, before[i])
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

func noopMessageIdBuilder() string {
	return ""
}

func noopTyper(_ interface{}) string {
	return ""
}

func DefaultTyper(v interface{}) string {
	return fmt.Sprintf("%T", v)
}
