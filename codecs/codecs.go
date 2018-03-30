package codecs

import (
	"sync"
)

const (
	JSONCodecName  = "application/json"
	ProtoCodecName = "application/protobuf"
	XMLCodecName   = "application/xml"
)

// Codec is an interface that encodes message on pub and decodes it on sub.
type Codec interface {
	Encoder
	Decoder
}
type Encoder interface {
	Encode(interface{}) ([]byte, error)
}
type Decoder interface {
	Decode([]byte, interface{}) error
}

var Register = register{codecs: make(map[string]Codec)}

func init() {
	Register.Register("", &JSONCodec{})
	Register.Register(JSONCodecName, &JSONCodec{})
	Register.Register(XMLCodecName, &XMLCodec{})
	Register.Register(ProtoCodecName, &ProtobufCodec{})
}

type register struct {
	mx     sync.Mutex
	codecs map[string]Codec
}

func (r *register) Register(contentType string, codec Codec) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.codecs[contentType] = codec
}

func (r *register) Get(contentType string) (Codec, bool) {
	codec, ok := r.codecs[contentType]
	return codec, ok
}
