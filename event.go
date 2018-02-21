package amqp

import (
	"context"

	"github.com/streadway/amqp"
)

type Event struct {
	d       amqp.Delivery
	Data    interface{}
	Context context.Context
}

func (e Event) Commit() {
	e.Ack(false)
}

func (e Event) Ack(multiple bool) error {
	return e.d.Ack(multiple)
}

func (e Event) Reject(requeue bool) error {
	return e.d.Reject(requeue)
}

func (e Event) Nack(multiple, requeue bool) error {
	return e.d.Nack(multiple, requeue)
}
