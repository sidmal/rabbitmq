package rabbitmq

import (
	"errors"
	"github.com/stretchr/testify/suite"
	"testing"
)

type TestFailMessageEncoder struct{}

func (m *TestFailMessageEncoder) GetContentType() string {
	return "test"
}

func (m *TestFailMessageEncoder) Marshal(_ interface{}) ([]byte, error) {
	return nil, errors.New("TestFailMessageEncoder_Marshal")
}

func (m *TestFailMessageEncoder) Unmarshal(_ []byte, _ interface{}) error {
	return errors.New("TestFailMessageEncoder_Unmarshal")
}

type MessageEncoderTestSuite struct {
	suite.Suite
	protoEncoder MessageEncoder
}

func Test_MessageEncoder(t *testing.T) {
	suite.Run(t, new(MessageEncoderTestSuite))
}

func (suite *MessageEncoderTestSuite) SetupTest() {
	suite.protoEncoder = NewProtobufEncoder()
}

func (suite *MessageEncoderTestSuite) TearDownTest() {}

func (suite *MessageEncoderTestSuite) TestMessageEncoder_ProtobufEncoder_Marshal_ErrorIncorrectMessageType() {
	_, err := suite.protoEncoder.Marshal(struct{}{})
	suite.Error(err)
	suite.Equal(ErrorIncorrectMessageType, err)
}

func (suite *MessageEncoderTestSuite) TestMessageEncoder_ProtobufEncoder_Unmarshal_ErrorIncorrectMessageType() {
	err := suite.protoEncoder.Unmarshal([]byte(`{}`), struct{}{})
	suite.Error(err)
	suite.Equal(ErrorIncorrectMessageType, err)
}
