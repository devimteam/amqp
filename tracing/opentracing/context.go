package opentracing

import (
	"context"
	"fmt"

	"github.com/devimteam/amqp"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	origin "github.com/streadway/amqp"
)

// ContextToAMQP injects span to AMQP headers section.
func ContextToAMQP(tracer opentracing.Tracer) amqp.PublishingBefore {
	return func(ctx context.Context, p *origin.Publishing) {
		span := opentracing.SpanFromContext(ctx)
		if span == nil {
			return
		}
		wr := amqpReaderWriter{headers: p.Headers}
		_ = tracer.Inject(span.Context(), opentracing.TextMap, &wr)
		p.Headers = wr.headers
	}
}

// AMQPToContextFabric return function that writes to context event's span.
//
// Example:
// tracer := ...
// fabric := amqpOpentracing.AMQPToContextFabric(tracer)
// client.Subscriber(amqp.SubscriberDeliverBefore(fabric("CreateComment"))
//
func AMQPToContextFabric(tracer opentracing.Tracer) func(string) amqp.DeliveryBefore {
	return func(operationName string) amqp.DeliveryBefore {
		return func(ctx context.Context, delivery *origin.Delivery) context.Context {
			// If an error occurred, span will be root span
			spanContext, _ := tracer.Extract(opentracing.TextMap, amqpReaderWriter{headers: delivery.Headers})
			span := tracer.StartSpan(operationName, ext.SpanKindConsumer, opentracing.FollowsFrom(spanContext))
			return opentracing.ContextWithSpan(ctx, span)
		}
	}
}

// A type that conforms to opentracing.TextMapReader and
// opentracing.TextMapWriter.
type amqpReaderWriter struct {
	headers map[string]interface{}
}

func (w amqpReaderWriter) ForeachKey(handler func(key, val string) error) error {
	for k, v := range w.headers {
		s, ok := v.(string)
		if !ok {
			stringer, ok := v.(fmt.Stringer)
			if !ok {
				continue
			}
			s = stringer.String()
		}
		if err := handler(k, s); err != nil {
			return err
		}
	}
	return nil
}

func (w *amqpReaderWriter) Set(key, val string) {
	if w.headers == nil {
		w.headers = make(map[string]interface{})
	}
	w.headers[key] = val
}
