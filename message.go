package amqp

import (
	"errors"
	"net/http"
	"time"

	"github.com/devimteam/amqp/codecs"
	"github.com/streadway/amqp"
)

type (
	ContentTyper interface {
		ContentType() string
	}

	subMessageOptions struct {
		deliveryBefore      []DeliveryBefore
		allowedContentTypes []string
		minPriority         uint8
		maxPriority         uint8
		defaultContentType  string
	}
)

var CodecNotFound = errors.New("codec not found")

// constructPublishing uses message options to construct amqp.Publishing.
func constructPublishing(v interface{}, priority uint8, defaultContentType string) (msg amqp.Publishing, err error) {
	msg.Timestamp = time.Now()
	msg.Priority = priority

	var contentType string
	ct, ok := v.(ContentTyper)
	if ok {
		msg.ContentType = ct.ContentType()
		contentType = msg.ContentType
	} else if defaultContentType != "" {
		msg.ContentType = defaultContentType
		contentType = defaultContentType
	} else {
		msg.ContentType = http.DetectContentType(msg.Body)
	}
	codec, ok := codecs.Register.Get(contentType)
	if !ok {
		return msg, CodecNotFound
	}
	msg.Body, err = codec.Encode(v)
	return
}
