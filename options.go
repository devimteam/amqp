package amqp

import (
	"context"
	"fmt"
	"time"

	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

const (
	MaxMessagePriority = 9
	MinMessagePriority = 0
)

const (
	defaultWaitDeadline = time.Second * 5
	defaultEventBuffer  = 1
	defaultWorkers      = 1
	defaultContentType  = "application/json"
)

type (
	// Function, that changes message before publishing.
	PublishingBefore func(context.Context, *amqp.Publishing)
	// Function, that changes message before delivering.
	DeliveryBefore func(context.Context, *amqp.Delivery) context.Context
	// Function, that changes error, which caused on incorrect handling.
	// Common use-case: debugging.
	ErrorBefore func(amqp.Delivery, error) error
)

// CommonTyper prints go-style type of value.
func CommonTyper(v interface{}) string {
	return fmt.Sprintf("%T", v)
}

// CommonMessageIdBuilder builds new UUID as message Id.
func CommonMessageIdBuilder() string {
	return uuid.NewV4().String()
}
