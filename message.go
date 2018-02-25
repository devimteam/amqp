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

	messageOptions struct {
		pubBefore           []PublishingBefore
		deliveryBefore      []DeliveryBefore
		applicationId       string
		userId              string
		idBuilder           MessageIdBuilder
		allowedContentTypes []string
		minPriority         uint8
		maxPriority         uint8
		typer               Typer
		defaultContentType  string
	}
)

var CodecNotFound = errors.New("codec not found")

// constructPublishing uses message options to construct amqp.Publishing.
func constructPublishing(v interface{}, opts *messageOptions) (msg amqp.Publishing, err error) {
	msg.AppId = opts.applicationId
	msg.MessageId = opts.idBuilder()
	msg.Timestamp = time.Now()
	msg.Type = opts.typer(v)

	var contentType string
	ct, ok := v.(ContentTyper)
	if ok {
		msg.ContentType = ct.ContentType()
		contentType = msg.ContentType
	} else if opts.defaultContentType != "" {
		msg.ContentType = opts.defaultContentType
		contentType = opts.defaultContentType
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
