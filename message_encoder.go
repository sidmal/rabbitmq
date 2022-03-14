package rabbitmq

import (
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/proto"
)

const (
	ProtobufContentType = "application/protobuf"
	JsonContentType     = "application/json"
)

var (
	ErrorIncorrectMessageType = errors.New("message to encoding has incorrect type")
)

type MessageEncoder interface {
	GetContentType() string
	Marshal(in interface{}) ([]byte, error)
	Unmarshal(in []byte, v interface{}) error
}

type ProtobufEncoder struct {
	ContentType string
}

type JsonEncoder struct {
	ContentType string
}

func NewProtobufEncoder() MessageEncoder {
	return &ProtobufEncoder{
		ContentType: ProtobufContentType,
	}
}

func NewJsonEncoder() MessageEncoder {
	return &JsonEncoder{
		ContentType: JsonContentType,
	}
}

func (m *ProtobufEncoder) Marshal(in interface{}) ([]byte, error) {
	msg, ok := in.(proto.Message)
	if !ok {
		return nil, ErrorIncorrectMessageType
	}

	return proto.Marshal(msg)
}

func (m *ProtobufEncoder) Unmarshal(in []byte, v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return ErrorIncorrectMessageType
	}

	return proto.Unmarshal(in, msg)
}

func (m *ProtobufEncoder) GetContentType() string {
	return m.ContentType
}

func (m *JsonEncoder) Marshal(in interface{}) ([]byte, error) {
	return json.Marshal(in)
}

func (m *JsonEncoder) Unmarshal(in []byte, v interface{}) error {
	return json.Unmarshal(in, v)
}

func (m *JsonEncoder) GetContentType() string {
	return m.ContentType
}
