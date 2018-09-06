package opentracing

import (
	"context"
	"fmt"

	"github.com/devimteam/amqp"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	origin "github.com/streadway/amqp"
)

func ContextToAMQP(tracer opentracing.Tracer) amqp.PublishingBefore {
	return func(ctx context.Context, p *origin.Publishing) {
		span := opentracing.SpanFromContext(ctx)
		if span == nil {
			return
		}
		tracer.Inject(span.Context(), opentracing.TextMap, amqpReaderWriter{headers: p.Headers})
	}
}

func AMQPToContextFabric(tracer opentracing.Tracer) func(string) amqp.DeliveryBefore {
	return func(operationName string) amqp.DeliveryBefore {
		return func(ctx context.Context, delivery *origin.Delivery) context.Context {
			// If an error occurred, span will be root span
			spanContext, _ := tracer.Extract(opentracing.TextMap, amqpReaderWriter{headers: delivery.Headers})
			span := tracer.StartSpan(operationName, ext.RPCServerOption(spanContext))
			return opentracing.ContextWithSpan(ctx, span)
		}
	}
}

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

func (w amqpReaderWriter) Set(key, val string) {
	w.headers[key] = val
}
