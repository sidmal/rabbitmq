package rabbitmq

import (
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sidmal/rabbitmq/test/proto"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

const (
	defaultAmqpUrl = "amqp://guest:guest@127.0.0.1:5672"
)

type BrokerTestSuite struct {
	suite.Suite
	broker BrokerInterface
}

func Test_Broker(t *testing.T) {
	suite.Run(t, new(BrokerTestSuite))
}

func (suite *BrokerTestSuite) SetupTest() {
	opts := []Option{
		DSN(defaultAmqpUrl),
		Topic("test"),
		MsgEncoder(NewProtobufEncoder()),
	}
	b, err := NewBroker(opts...)
	suite.NoError(err)
	suite.NotNil(b)

	broker, ok := b.(*Broker)
	suite.True(ok)
	suite.Equal(defaultAmqpUrl, broker.opts.dsn)
	suite.NotNil(broker.rabbitMQ)
	suite.NotNil(broker.consumer)

	suite.broker = b
}

func (suite *BrokerTestSuite) TearDownTest() {
	err := suite.broker.QueuePurge()
	suite.NoError(err)
}

func (suite *BrokerTestSuite) TestBroker_NewBroker_TopicNameEmpty_Error() {
	_, err := NewBroker()
	suite.Error(err)
	suite.Equal(ErrorTopicNameEmpty, err)
}

func (suite *BrokerTestSuite) TestBroker_NewBroker_NewRabbitMq_Error() {
	opts := []Option{
		Topic("test"),
	}
	_, err := NewBroker(opts...)
	suite.Error(err)
	suite.Equal(ErrorDsnEmpty, err)
}

func (suite *BrokerTestSuite) TestBroker_AddConsumerHandler_Ok() {
	suite.Len(suite.broker.(*Broker).consumer.handlers, 0)
	fn := func(msg interface{}, b amqp.Delivery) error {
		return nil
	}
	err := suite.broker.AddConsumerHandler(&proto.One{}, fn)
	suite.NoError(err)
	suite.Len(suite.broker.(*Broker).consumer.handlers, 1)
}

func (suite *BrokerTestSuite) TestBroker_AddConsumerHandler_HandlerDuplicate() {
	suite.Len(suite.broker.(*Broker).consumer.handlers, 0)
	fn := func(msg interface{}, b amqp.Delivery) error {
		return nil
	}
	err := suite.broker.AddConsumerHandler(&proto.One{}, fn)
	suite.NoError(err)
	suite.Len(suite.broker.(*Broker).consumer.handlers, 1)

	err = suite.broker.AddConsumerHandler(&proto.One{}, fn)
	suite.Error(err)
	suite.Equal(ErrorConsumerFnExists, err)
	suite.Len(suite.broker.(*Broker).consumer.handlers, 1)
}

func (suite *BrokerTestSuite) TestBroker_AddConsumerHandler_HandlerIncorrectFirstArgTypes() {
	suite.Len(suite.broker.(*Broker).consumer.handlers, 0)
	fn0 := func(msg0 interface{}, b amqp.Delivery) error { return nil }
	fn1 := func(msg1 interface{}, b amqp.Delivery) error { return nil }
	err := suite.broker.AddConsumerHandler(&proto.One{}, fn0)
	suite.NoError(err)
	suite.Len(suite.broker.(*Broker).consumer.handlers, 1)

	err = suite.broker.AddConsumerHandler(&proto.Two{}, fn1)
	suite.Error(err)
	suite.Equal(ErrorConsumerFirstArgNotMatch, err)
	suite.Len(suite.broker.(*Broker).consumer.handlers, 1)
}

func (suite *BrokerTestSuite) TestBroker_AddConsumerHandler_HandlerMoreOneHandlersCorrect() {
	suite.Len(suite.broker.(*Broker).consumer.handlers, 0)
	fns := []func(msg interface{}, b amqp.Delivery) error{
		func(msg0 interface{}, b amqp.Delivery) error { return nil },
		func(msg1 interface{}, b amqp.Delivery) error { return nil },
		func(msg2 interface{}, b amqp.Delivery) error { return nil },
		func(msg3 interface{}, b amqp.Delivery) error { return nil },
		func(msg4 interface{}, b amqp.Delivery) error { return nil },
	}

	for _, val := range fns {
		err := suite.broker.AddConsumerHandler(&proto.One{}, val)
		suite.NoError(err)
	}
	suite.Len(suite.broker.(*Broker).consumer.handlers, 5)
}

func (suite *BrokerTestSuite) TestBroker_AddConsumerHandler_FirstArgNotPointer_Error() {
	suite.Len(suite.broker.(*Broker).consumer.handlers, 0)
	fn0 := func(msg0 interface{}, b amqp.Delivery) error { return nil }
	err := suite.broker.AddConsumerHandler(proto.One{}, fn0)
	suite.Error(err)
	suite.Equal(ErrorFirsArgMustBePointer, err)
	suite.Len(suite.broker.(*Broker).consumer.handlers, 0)
}

func (suite *BrokerTestSuite) TestBroker_Publish_Ok() {
	msg := &proto.One{
		Value: "test",
	}
	err := suite.broker.Publish(msg, nil)
	suite.NoError(err)
}

func (suite *BrokerTestSuite) TestBroker_Publish_EncoderMarshal_Error() {
	opts := []Option{
		DSN(defaultAmqpUrl),
		Topic("test"),
		MsgEncoder(&TestFailMessageEncoder{}),
	}
	b, err := NewBroker(opts...)
	suite.NoError(err)
	suite.NotNil(b)

	msg := &proto.One{
		Value: "test",
	}
	err = b.Publish(msg, nil)
	suite.Error(err)
	suite.EqualError(err, "TestFailMessageEncoder_Marshal")
}

func (suite *BrokerTestSuite) TestBroker_StartConsume_Ok() {
	inConsumerFn := false
	fn := func(msg interface{}, b amqp.Delivery) error {
		event, ok := msg.(*proto.One)
		suite.True(ok)
		suite.Equal(event.Value, "test message")
		inConsumerFn = true
		return nil
	}
	err := suite.broker.AddConsumerHandler(&proto.One{}, fn)
	suite.NoError(err)

	msg := &proto.One{
		Value: "test message",
	}
	err = suite.broker.Publish(msg, nil)
	suite.NoError(err)

	tp := time.NewTimer(time.Second * 1)
	quit := make(chan struct{}, 1)
	exit := make(chan struct{}, 1)

	go func(done chan struct{}) {
		err = suite.broker.StartConsume(quit)
		suite.NoError(err)
	}(quit)

	select {
	case <-tp.C:
		quit <- struct{}{}
		exit <- struct{}{}
	}
	<-exit

	suite.True(inConsumerFn)
}

func (suite *BrokerTestSuite) TestBroker_ConsumerFuncError_Ok() {
	opts := []Option{
		DSN(defaultAmqpUrl),
		Topic("test1"),
		Queue(&QueueOptions{
			Name: "test1.queue",
			Opts: DefaultQueueOpts,
		}),
		Exchange(&ExchangeOptions{
			Name: "test1.exchange",
			Kind: "topic",
			Opts: DefaultExchangeOpts,
		}),
		Consumer(&ConsumerOptions{
			Opts: DefaultConsumerOpts,
		}),
		QueueBind(&QueueBindOptions{
			Key:    "*",
			NoWait: false,
		}),
		Publisher(&PublisherOptions{Opts: DefaultPublisherOpts}),
	}
	broker, err := NewBroker(opts...)

	inConsumerFn := false
	fn := func(msg interface{}, b amqp.Delivery) error {
		inConsumerFn = true
		return errors.New("TestBroker_ConsumerFuncError_Ok")
	}
	err = broker.AddConsumerHandler(&proto.One{}, fn)
	suite.NoError(err)

	msg := &proto.One{
		Value: "test message",
	}
	err = broker.Publish(msg, nil)
	suite.NoError(err)

	tp := time.NewTimer(time.Second * 1)
	quit := make(chan struct{}, 1)
	exit := make(chan struct{}, 1)

	go func(done chan struct{}) {
		err = broker.StartConsume(quit)
		suite.NoError(err)
	}(quit)

	select {
	case <-tp.C:
		quit <- struct{}{}
		exit <- struct{}{}
	}
	<-exit

	suite.True(inConsumerFn)
}
