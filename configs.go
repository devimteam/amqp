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
		Name:       "queue" + genRandomString(15),
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
	exchangeConfig  ExchangeConfig
	queueConfig     QueueConfig
	queueBindConfig QueueBindConfig
	consumeConfig   ConsumeConfig
	publishConfig   PublishConfig
}

func newConfigs() configs {
	return configs{
		exchangeConfig:  DefaultExchangeConfig(),
		queueConfig:     DefaultQueueConfig(),
		queueBindConfig: DefaultQueueBindConfig(),
		consumeConfig:   DefaultConsumeConfig(),
		publishConfig:   DefaultPublishConfig(),
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

func channelExchangeDeclare(channel *amqp.Channel, exchangeName string, exchangeCfg ExchangeConfig) error {
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

func channelQueueDeclare(channel *amqp.Channel, queueCfg QueueConfig) (amqp.Queue, error) {
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
