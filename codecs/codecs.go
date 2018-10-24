package codecs

import (
	"sync"
)

const (
	JSONCodecName = "application/json"
	// check https://tools.ietf.org/html/draft-rfernando-protocol-buffers-00 for more information.
	// but it still a draft, so this constant can be renamed in any future release:
	// when some mime type for protobuf will be accepted as a standard.
	ProtoCodecName  = "application/protobuf"
	ProtoCodecName1 = "application/x-protobuf"        // will be linked to ProtoCodecName in future release.
	ProtoCodecName2 = "application/x-google-protobuf" // will be linked to ProtoCodecName in future release.
	XMLCodecName    = "application/xml"
)

type (
	// Codec is an interface that encodes message on pub and decodes it on sub.
	Codec interface {
		Encoder
		Decoder
	}
	Encoder interface {
		Encode(interface{}) ([]byte, error)
	}
	Decoder interface {
		Decode([]byte, interface{}) error
	}
)

var Register = register{codecs: make(map[string]Codec)}

func init() {
	Register.Register("", &JSONCodec{})
	Register.Register(JSONCodecName, &JSONCodec{})
	Register.Register(XMLCodecName, &XMLCodec{})
	Register.Register(ProtoCodecName, &ProtobufCodec{})
	Register.Register(ProtoCodecName1, &ProtobufCodec{})
	Register.Register(ProtoCodecName2, &ProtobufCodec{})
}

type register struct {
	mx     sync.RWMutex
	codecs map[string]Codec
}

func (r *register) Register(contentType string, codec Codec) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.codecs[contentType] = codec
}

func (r *register) Get(contentType string) (Codec, bool) {
	r.mx.RLock()
	defer r.mx.RUnlock()
	codec, ok := r.codecs[contentType]
	return codec, ok
}
