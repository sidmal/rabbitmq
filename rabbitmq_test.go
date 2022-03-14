package rabbitmq

import (
	"github.com/sidmal/rabbitmq/test/proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRabbitMq_NewRabbitMq_Connect_Error(t *testing.T) {
	opts := &Options{
		dsn: "bla-bla-bla",
	}
	_, err := newRabbitMq(opts)
	assert.Error(t, err)
}

func TestRabbitMq_DeclareInfrastructure_ExchangeDeclare_Error(t *testing.T) {
	opts := &Options{
		dsn: defaultAmqpUrl,
		exchange: &ExchangeOptions{
			Args: map[string]interface{}{
				"some_undefined_arg": &proto.One{},
			},
		},
	}
	rmq := &rabbitMq{
		dsn:  defaultAmqpUrl,
		opts: opts,
	}
	conn, ch, err := rmq.connect()
	assert.NoError(t, err)
	rmq.conn = conn
	rmq.channel = ch

	err = rmq.declareInfrastructure()
	assert.Error(t, err)
}

func TestRabbitMq_DeclareInfrastructure_QueueDeclare_Error(t *testing.T) {
	opts := &Options{
		dsn: defaultAmqpUrl,
		queue: &QueueOptions{
			Args: map[string]interface{}{
				"some_undefined_arg": &proto.One{},
			},
		},
	}
	rmq := &rabbitMq{
		dsn:  defaultAmqpUrl,
		opts: opts,
	}
	conn, ch, err := rmq.connect()
	assert.NoError(t, err)
	rmq.conn = conn
	rmq.channel = ch

	err = rmq.declareInfrastructure()
	assert.Error(t, err)
}

func TestRabbitMq_Reconnect_ConnectSleep_Ok(t *testing.T) {
	opts := &Options{
		dsn: defaultAmqpUrl,
	}
	rmq, err := newRabbitMq(opts)
	assert.NoError(t, err)

	rmq.connected = false
	rmq.waitConnection = make(chan struct{})
	rmq.dsn = "bla-bla-bla"

	tp := time.NewTimer(time.Second * 2)
	tp1 := time.NewTimer(time.Second * 1)
	exit := make(chan struct{}, 1)
	go func() {
		rmq.reconnect()
	}()

	select {
	case <-tp.C:
		rmq.close <- true
		exit <- struct{}{}
		break
	}

	select {
	case <-tp1.C:
		rmq.opts.dsn = defaultAmqpUrl
		break
	}
	<-exit
}

func TestRabbitMq_Reconnect_ConnectWithoutSleep_Ok(t *testing.T) {
	opts := &Options{
		dsn: defaultAmqpUrl,
	}
	rmq, err := newRabbitMq(opts)
	assert.NoError(t, err)

	rmq.connected = false
	rmq.waitConnection = make(chan struct{})

	tp := time.NewTimer(time.Second * 1)
	exit := make(chan struct{}, 1)
	go func() {
		rmq.reconnect()
	}()

	select {
	case <-tp.C:
		rmq.close <- true
		exit <- struct{}{}
		break
	}
	<-exit
}

func TestRabbitMq_Reconnect_CloseConnect(t *testing.T) {
	opts := &Options{
		dsn: defaultAmqpUrl,
	}
	rmq, err := newRabbitMq(opts)
	assert.NoError(t, err)

	tp := time.NewTimer(time.Second * 2)
	tp1 := time.NewTimer(time.Second * 1)
	exit := make(chan struct{}, 1)
	go func() {
		rmq.reconnect()
	}()

	select {
	case <-tp.C:
		rmq.close <- true
		exit <- struct{}{}
		break
	}

	select {
	case <-tp1.C:
		_ = rmq.conn.Close()
		break
	}
	<-exit
}
