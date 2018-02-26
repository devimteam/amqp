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

func channelPublish(channel *amqp.Channel, exchangeName string, cfg PublishConfig, msg amqp.Publishing) error {
	return channel.Publish(
		exchangeName,
		cfg.Key,
		cfg.Mandatory,
		cfg.Immediate,
		msg,
	)
}

func channelConsume(channel *amqp.Channel, queueName string, consumeCfg ConsumeConfig) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queueName,
		consumeCfg.Consumer,
		consumeCfg.AutoAck,
		consumeCfg.Exclusive,
		consumeCfg.NoLocal,
		consumeCfg.NoWait,
		consumeCfg.Args,
	)
}

func (c Client) channelExchangeDeclare(channel *amqp.Channel, exchangeName string, exchangeCfg ExchangeConfig) (err error) {
	if c.opts.lazy.declaring && c.opts.lazy.exchangesDeclared.Find(exchangeName) >= 0 {
		return nil
	}
	defer func() {
		if err == nil && c.opts.lazy.declaring && c.opts.lazy.exchangesDeclared.Find(exchangeName) == -1 {
			c.opts.lazy.exchangesDeclared.Append(exchangeName)
		}
	}()
	return channel.ExchangeDeclare(
		exchangeName,
		exchangeCfg.Kind,
		exchangeCfg.Durable,
		exchangeCfg.AutoDelete,
		exchangeCfg.Internal,
		exchangeCfg.NoWait,
		exchangeCfg.Args,
	)
}

func (c Client) channelQueueDeclare(channel *amqp.Channel, queueCfg QueueConfig) (q amqp.Queue, err error) {
	if c.opts.lazy.declaring && c.opts.lazy.queueDeclared.Find(queueCfg.Name) >= 0 {
		return amqp.Queue{Name: queueCfg.Name}, nil
	}
	defer func() {
		if err == nil && c.opts.lazy.declaring && c.opts.lazy.queueDeclared.Find(q.Name) == -1 {
			c.opts.lazy.queueDeclared.Append(q.Name)
		}
	}()
	return channel.QueueDeclare(
		queueCfg.Name,
		queueCfg.Durable,
		queueCfg.AutoDelete,
		queueCfg.Exclusive,
		queueCfg.NoWait,
		queueCfg.Args,
	)
}

func channelQueueBind(channel *amqp.Channel, queueName, exchangeName string, queueBindCfg QueueBindConfig) error {
	return channel.QueueBind(
		queueName,
		queueBindCfg.Key,
		exchangeName,
		queueBindCfg.NoWait,
		queueBindCfg.Args,
	)
}

// todo: add common configurations for: 1. durable exchange, where every event is important, 2. exchange with temporary queries
