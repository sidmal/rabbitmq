//go:build !race

package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sidmal/rabbitmq/test/proto"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func TestConsumer_ReConsume_MayRunFalse(t *testing.T) {
	cons := &consumer{}
	cons.reConsume()
}

func TestConsumer_ReConsume_RabbitMQClose(t *testing.T) {
	cons := &consumer{
		mayRun: true,
		rabbitMQ: &rabbitMq{
			close:          make(chan bool, 1),
			waitConnection: make(chan struct{}, 1),
		},
	}
	cons.rabbitMQ.waitConnection <- struct{}{}

	tp := time.NewTimer(time.Second * 1)
	exit := make(chan struct{}, 1)
	go func() {
		cons.reConsume()
	}()

	select {
	case <-tp.C:
		cons.rabbitMQ.close <- true
		exit <- struct{}{}
	}
	<-exit
}

func TestConsumer_ReConsume_ConsumeError(t *testing.T) {
	opts := &Options{
		dsn: defaultAmqpUrl,
		consumer: &ConsumerOptions{
			Args: map[string]interface{}{
				"some_undefined_arg": &proto.One{},
			},
		},
		topic: "TestConsumer_ReConsume_ConsumeError",
	}
	rmq, err := newRabbitMq(opts)
	assert.NoError(t, err)
	cons := &consumer{
		mayRun:              true,
		rabbitMQ:            rmq,
		minResubscribeDelay: 100 * time.Millisecond,
		maxResubscribeDelay: 1 * time.Second,
	}

	tp := time.NewTimer(time.Second * 2)
	exit := make(chan struct{}, 1)
	go func() {
		cons.reConsume()
	}()

	select {
	case <-tp.C:
		cons.rabbitMQ.close <- true
		exit <- struct{}{}
	}
	<-exit
}

func TestConsumer_HandlerExecute_Error(t *testing.T) {
	msg, err := NewProtobufEncoder().Marshal(&proto.One{Value: "test"})
	assert.NoError(t, err)
	publishing := amqp.Delivery{
		Body: msg,
	}
	h := &handler{
		msgType: reflect.TypeOf(&proto.One{}).Elem(),
	}
	err = h.execute(publishing, &TestFailMessageEncoder{})
	assert.Error(t, err)
	assert.EqualError(t, err, "TestFailMessageEncoder_Unmarshal")
}
