// Please, check https://www.rabbitmq.com/amqp-0-9-1-reference.html to learn about configs parameters.

package amqp

import "github.com/streadway/amqp"

type ExchangeConfig struct {
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func DefaultExchangeConfig() ExchangeConfig {
	return ExchangeConfig{
		Kind:       "fanout",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}

// LongExchangeConfig should be used as configure shortcut for long-live exchanges.
func LongExchangeConfig() ExchangeConfig {
	return ExchangeConfig{
		Kind:       "fanout",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	IfUnused   bool
	IfEmpty    bool
	Args       amqp.Table
}

func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		Name:       "",
		Durable:    false,
		AutoDelete: true,
		Exclusive:  false,
		NoWait:     false,
		IfUnused:   false,
		IfEmpty:    false,
		Args:       nil,
	}
}

// LongQueueConfig should be used as configure shortcut for long-live queues.
func LongQueueConfig(name string) QueueConfig {
	return QueueConfig{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		IfUnused:   false,
		IfEmpty:    false,
		Args:       nil,
	}
}

type QueueBindConfig struct {
	Key    string
	NoWait bool
	Args   amqp.Table
}

func DefaultQueueBindConfig() QueueBindConfig {
	return QueueBindConfig{
		Key:    "",
		NoWait: false,
		Args:   nil,
	}
}

type ConsumeConfig struct {
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

func DefaultConsumeConfig() ConsumeConfig {
	return ConsumeConfig{
		Consumer:  "",
		AutoAck:   false,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Args:      nil,
	}
}

type PublishConfig struct {
	Key       string
	Mandatory bool
	Immediate bool
}

func DefaultPublishConfig() PublishConfig {
	return PublishConfig{
		Key:       "",
		Immediate: false,
		Mandatory: false,
	}
}

type configs struct {
	exchange  ExchangeConfig
	queue     QueueConfig
	queueBind QueueBindConfig
	consume   ConsumeConfig
	publish   PublishConfig
	conn      *amqp.Config
}

func newConfigs() configs {
	return configs{
		exchange:  DefaultExchangeConfig(),
		queue:     DefaultQueueConfig(),
		queueBind: DefaultQueueBindConfig(),
		consume:   DefaultConsumeConfig(),
		publish:   DefaultPublishConfig(),
	}
}

// todo: add common configurations for: 1. durable exchange, where every event is important, 2. exchange with temporary queries
