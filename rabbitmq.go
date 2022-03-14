package rabbitmq

import (
	"errors"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

var (
	ErrorDsnEmpty = errors.New("message broker DSN url can't be empty")
)

type rabbitMq struct {
	dsn     string
	conn    *amqp.Connection
	channel *amqp.Channel
	uuid    string
	wg      sync.WaitGroup
	sync.Mutex
	connected      bool
	close          chan bool
	waitConnection chan struct{}
	opts           *Options
}

func newRabbitMq(opts *Options) (*rabbitMq, error) {
	var err error

	if opts.dsn == "" {
		return nil, ErrorDsnEmpty
	}

	rmq := &rabbitMq{
		dsn:            opts.dsn,
		uuid:           uuid.NewString(),
		close:          make(chan bool),
		waitConnection: make(chan struct{}),
		opts:           opts,
	}

	rmq.conn, rmq.channel, err = rmq.connect()
	if err != nil {
		return nil, err
	}
	close(rmq.waitConnection)

	rmq.Lock()
	rmq.connected = true
	rmq.Unlock()

	go rmq.reconnect()
	return rmq, rmq.declareInfrastructure()
}

func (m *rabbitMq) connect() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(m.dsn)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}

func (m *rabbitMq) declareInfrastructure() error {
	exchOpts := m.opts.GetExchangeOpts()
	exchName := m.opts.GetExchangeName()
	err := m.channel.ExchangeDeclare(exchName, m.opts.GetExchangeKind(), exchOpts.Durable, exchOpts.AutoDelete,
		exchOpts.Internal, exchOpts.NoWait, m.opts.GetExchangeArgs())
	if err != nil {
		return err
	}

	queueName := m.opts.GetQueueName()
	queueOpts := m.opts.GetQueueOpts()
	_, err = m.channel.QueueDeclare(queueName, queueOpts.Durable, queueOpts.AutoDelete, queueOpts.Exclusive,
		queueOpts.NoWait, m.opts.GetQueueArgs())
	if err != nil {
		return err
	}

	return m.channel.QueueBind(queueName, m.opts.GetQueueBindKey(), exchName, m.opts.GetQueueBindNoWait(),
		m.opts.GetQueueBindArgs())
}

func (m *rabbitMq) reconnect() {
	for {
		m.Lock()
		connected := m.connected
		m.Unlock()

		if !connected {
			if _, _, err := m.connect(); err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			m.Lock()
			m.connected = true
			m.Unlock()

			close(m.waitConnection)
		}

		notifyClose := make(chan *amqp.Error)
		m.conn.NotifyClose(notifyClose)

		select {
		case <-notifyClose:
			m.Lock()
			m.connected = false
			m.waitConnection = make(chan struct{})
			m.Unlock()
		case <-m.close:
			_ = m.channel.Cancel(m.uuid, true)
			_ = m.conn.Close()
			return
		}
	}
}

func (m *rabbitMq) consume() (<-chan amqp.Delivery, error) {
	consOpts := m.opts.GetConsumerOpts()
	return m.channel.Consume(m.opts.GetQueueName(), m.uuid, consOpts.AutoAck, consOpts.Exclusive,
		consOpts.NoLocal, consOpts.NoWait, m.opts.GetConsumerArgs())
}

func (m *rabbitMq) queuePurge() error {
	_, err := m.channel.QueuePurge(m.opts.GetQueueName(), m.opts.GetQueueOpts().NoWait)
	return err
}
