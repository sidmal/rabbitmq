package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"reflect"
	"sync"
	"time"
)

type ConsumerFn func(decMsg interface{}, delMsg amqp.Delivery) error

type handler struct {
	fn      ConsumerFn
	msgType reflect.Type
}

type consumer struct {
	handlers []*handler
	fn       func(msg amqp.Delivery)
	sync.Mutex
	mayRun              bool
	rabbitMQ            *rabbitMq
	opts                *Options
	existsHandlers      map[string]struct{}
	encoder             MessageEncoder
	minResubscribeDelay time.Duration
	maxResubscribeDelay time.Duration
}

func newConsumer(rmq *rabbitMq, encoder MessageEncoder, opts *Options) *consumer {
	cons := &consumer{
		rabbitMQ:            rmq,
		handlers:            []*handler{},
		existsHandlers:      make(map[string]struct{}),
		encoder:             encoder,
		opts:                opts,
		minResubscribeDelay: 100 * time.Millisecond,
		maxResubscribeDelay: 30 * time.Second,
	}
	return cons
}

func (m *consumer) Consume() {
	autoAck := m.opts.GetConsumerOpts().AutoAck
	fn := func(in amqp.Delivery) {
		for _, h := range m.handlers {
			err := h.execute(in, m.encoder)
			if err != nil {
				if !autoAck {
					_ = in.Nack(false, false)
				}
				continue
			}

			if !autoAck {
				_ = in.Ack(false)
			}
		}
	}

	m.fn = fn
	m.mayRun = true
	go m.reConsume()
}

func (m *consumer) reConsume() {
	expFactor := time.Duration(2)
	reSubscribeDelay := m.minResubscribeDelay

	for {
		m.Lock()
		mayRun := m.mayRun
		m.Unlock()

		if !mayRun {
			return
		}

		select {
		case <-m.rabbitMQ.close:
			return
		case <-m.rabbitMQ.waitConnection:
		}

		m.rabbitMQ.Lock()
		if !m.rabbitMQ.connected {
			m.rabbitMQ.Unlock()
			continue
		}

		cons, err := m.rabbitMQ.consume()
		m.rabbitMQ.Unlock()

		switch err {
		case nil:
			reSubscribeDelay = m.minResubscribeDelay
		default:
			if reSubscribeDelay > m.maxResubscribeDelay {
				reSubscribeDelay = m.maxResubscribeDelay
			}
			time.Sleep(reSubscribeDelay)
			reSubscribeDelay *= expFactor
			continue
		}

		for d := range cons {
			m.rabbitMQ.wg.Add(1)

			go func(d amqp.Delivery) {
				m.fn(d)
				m.rabbitMQ.wg.Done()
			}(d)
		}
	}
}

func (m *handler) execute(in amqp.Delivery, encoder MessageEncoder) error {
	msg := reflect.New(m.msgType).Interface()
	err := encoder.Unmarshal(in.Body, msg)
	if err != nil {
		return err
	}

	return m.fn(msg, in)
}
