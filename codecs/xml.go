package codecs

import "encoding/xml"

type XMLCodec struct {
}

func (c *XMLCodec) Encode(v interface{}) ([]byte, error) {
	return xml.Marshal(v)
}

func (c *XMLCodec) Decode(data []byte, v interface{}) error {
	return xml.Unmarshal(data, v)
}
