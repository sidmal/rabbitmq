package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultExchangeKind = "topic"
	DefaultQueueBindKey = "*"
)

var (
	DefaultEncoder      = NewJsonEncoder()
	DefaultExchangeOpts = &ExchangeOpts{
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
	}
	DefaultQueueOpts = &QueueOpts{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
	}
	DefaultConsumerOpts = &ConsumerOpts{
		AutoAck:   false,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
	}
	DefaultPublisherOpts = &PublisherOpts{
		Mandatory: false,
		Immediate: false,
	}
)

type ExchangeOpts struct {
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
}

type QueueOpts struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
}

type ConsumerOpts struct {
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
}

type PublisherOpts struct {
	Mandatory bool
	Immediate bool
}

type Options struct {
	dsn       string
	topic     string
	encoder   MessageEncoder
	queue     *QueueOptions
	exchange  *ExchangeOptions
	queueBind *QueueBindOptions
	consumer  *ConsumerOptions
	publisher *PublisherOptions
}

type Option func(*Options)

type QueueOptions struct {
	Name string
	Opts *QueueOpts
	Args amqp.Table
}

type ExchangeOptions struct {
	Name string
	Kind string
	Opts *ExchangeOpts
	Args amqp.Table
}

type QueueBindOptions struct {
	Key    string
	NoWait bool
	Args   amqp.Table
}

type ConsumerOptions struct {
	Opts *ConsumerOpts
	Args amqp.Table
}

type PublisherOptions struct {
	Opts *PublisherOpts
}

func DSN(val string) Option {
	return func(opts *Options) {
		opts.dsn = val
	}
}

func Topic(val string) Option {
	return func(opts *Options) {
		opts.topic = val
	}
}

func MsgEncoder(val MessageEncoder) Option {
	return func(opts *Options) {
		opts.encoder = val
	}
}

func Queue(val *QueueOptions) Option {
	return func(opts *Options) {
		opts.queue = val
	}
}

func Exchange(val *ExchangeOptions) Option {
	return func(opts *Options) {
		opts.exchange = val
	}
}

func QueueBind(val *QueueBindOptions) Option {
	return func(opts *Options) {
		opts.queueBind = val
	}
}

func Consumer(val *ConsumerOptions) Option {
	return func(opts *Options) {
		opts.consumer = val
	}
}

func Publisher(val *PublisherOptions) Option {
	return func(opts *Options) {
		opts.publisher = val
	}
}

func (m *Options) GetMessageEncoder() MessageEncoder {
	if m.encoder == nil {
		return DefaultEncoder
	}
	return m.encoder
}

func (m *Options) GetQueueName() string {
	if m.queue == nil || m.queue.Name == "" {
		return m.GetExchangeName() + ".queue"
	}
	return m.queue.Name
}

func (m *Options) GetExchangeName() string {
	if m.exchange == nil || m.exchange.Name == "" {
		return m.topic
	}
	return m.exchange.Name
}

func (m *Options) GetExchangeKind() string {
	if m.exchange == nil || m.exchange.Kind == "" {
		return DefaultExchangeKind
	}
	return m.exchange.Kind
}

func (m *Options) GetQueueBindKey() string {
	if m.queueBind == nil || m.queueBind.Key == "" {
		return DefaultQueueBindKey
	}
	return m.queueBind.Key
}

func (m *Options) GetQueueOpts() *QueueOpts {
	if m.queue == nil || m.queue.Opts == nil {
		return DefaultQueueOpts
	}
	return m.queue.Opts
}

func (m *Options) GetExchangeOpts() *ExchangeOpts {
	if m.exchange == nil || m.exchange.Opts == nil {
		return DefaultExchangeOpts
	}
	return m.exchange.Opts
}

func (m *Options) GetConsumerOpts() *ConsumerOpts {
	if m.consumer == nil || m.consumer.Opts == nil {
		return DefaultConsumerOpts
	}
	return m.consumer.Opts
}

func (m *Options) GetPublisherOpts() *PublisherOpts {
	if m.publisher == nil || m.publisher.Opts == nil {
		return DefaultPublisherOpts
	}
	return m.publisher.Opts
}

func (m *Options) GetQueueArgs() amqp.Table {
	if m.queue == nil {
		return nil
	}
	return m.queue.Args
}

func (m *Options) GetExchangeArgs() amqp.Table {
	if m.exchange == nil {
		return nil
	}
	return m.exchange.Args
}

func (m *Options) GetQueueBindArgs() amqp.Table {
	if m.queueBind == nil {
		return nil
	}
	return m.queueBind.Args
}

func (m *Options) GetQueueBindNoWait() bool {
	if m.queueBind == nil {
		return false
	}
	return m.queueBind.NoWait
}

func (m *Options) GetConsumerArgs() amqp.Table {
	if m.consumer == nil {
		return nil
	}
	return m.consumer.Args
}
