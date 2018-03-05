package amqp

import "github.com/streadway/amqp"

type Declaration interface {
	Declare(channel *Channel) error
}

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func (c Exchange) Declare(ch *Channel) error {
	ch.channel.ExchangeDeclare()
}
