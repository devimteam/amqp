package codecs

import (
	"errors"

	"github.com/gogo/protobuf/proto"
)

var NotProtoMessage = errors.New("not proto message")

type ProtobufCodec struct {
}

func (c *ProtobufCodec) Encode(v interface{}) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, NotProtoMessage
	}
	return proto.Marshal(msg)
}

func (c *ProtobufCodec) Decode(data []byte, v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return NotProtoMessage
	}
	return proto.Unmarshal(data, msg)
}
