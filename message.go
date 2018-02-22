package amqp

import (
	"errors"
	"net/http"
	"time"

	"github.com/devimteam/amqp/codecs"
	"github.com/streadway/amqp"
)

type ContentTyper interface {
	ContentType() string
}

type messageOptions struct {
	pubBefore           []PublishingBefore
	delBefore           []DeliveryBefore
	applicationId       string
	userId              string
	idBuilder           MessageIdBuilder
	allowedContentTypes []string
	minPriority         uint8
	maxPriority         uint8
	typer               Typer
}

var CodecNotFound = errors.New("codec not found")

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
