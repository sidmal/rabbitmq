package rabbitmq

import (
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"syscall"
)

var (
	ErrorTopicNameEmpty           = errors.New("topic name can't be empty")
	ErrorConsumerFnExists         = errors.New("consumer func already exists")
	ErrorConsumerFirstArgNotMatch = errors.New("consumer functions first arguments must have matched types")
	ErrorFirsArgMustBePointer     = errors.New("first argument of handler func must be pointer")
)

type BrokerInterface interface {
	AddConsumerHandler(msg interface{}, handlerFn ConsumerFn) error
	StartConsume(quit chan struct{}) error
	Publish(msg interface{}, headers amqp.Table) error
	QueuePurge() error
}

type Broker struct {
	rabbitMQ *rabbitMq
	consumer *consumer
	encoder  MessageEncoder
	opts     *Options
}

func NewBroker(opts ...Option) (BrokerInterface, error) {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.topic == "" {
		return nil, ErrorTopicNameEmpty
	}

	rmq, err := newRabbitMq(options)
	if err != nil {
		return nil, err
	}

	encoder := options.GetMessageEncoder()
	broker := &Broker{
		rabbitMQ: rmq,
		consumer: newConsumer(rmq, encoder, options),
		encoder:  encoder,
		opts:     options,
	}
	return broker, nil
}

func (m *Broker) AddConsumerHandler(msg interface{}, handlerFn ConsumerFn) error {
	refFn := reflect.ValueOf(handlerFn)
	msgType := reflect.TypeOf(msg)
	if msgType.Kind() != reflect.Ptr {
		return ErrorFirsArgMustBePointer
	}

	msgTypeEl := reflect.TypeOf(msg).Elem()
	fnName := runtime.FuncForPC(refFn.Pointer()).Name()

	if _, ok := m.consumer.existsHandlers[fnName]; ok {
		return ErrorConsumerFnExists
	}

	if len(m.consumer.handlers) > 0 {
		if m.consumer.handlers[0].msgType != msgTypeEl {
			return ErrorConsumerFirstArgNotMatch
		}
	}

	h := &handler{
		fn:      handlerFn,
		msgType: msgTypeEl,
	}
	m.consumer.handlers = append(m.consumer.handlers, h)
	m.consumer.existsHandlers[fnName] = struct{}{}

	return nil
}

func (m *Broker) StartConsume(quit chan struct{}) error {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	m.consumer.Consume()

	select {
	case <-ch:
	case <-quit:
		m.rabbitMQ.close <- true
	}

	return nil
}

func (m *Broker) Publish(msg interface{}, headers amqp.Table) error {
	var err error

	if headers == nil {
		headers = make(amqp.Table)
	}

	publication := amqp.Publishing{
		ContentType: m.encoder.GetContentType(),
		Headers:     headers,
	}
	publication.Body, err = m.encoder.Marshal(msg)
	if err != nil {
		return err
	}

	pubOpts := m.opts.GetPublisherOpts()
	return m.rabbitMQ.channel.Publish(m.opts.GetExchangeName(), m.opts.topic, pubOpts.Mandatory, pubOpts.Immediate,
		publication)
}

func (m *Broker) QueuePurge() error {
	return m.rabbitMQ.queuePurge()
}
