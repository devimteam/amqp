package opentracing

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/devimteam/amqp"
	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"
	amqp2 "github.com/streadway/amqp"
)

var tracer opentracing.Tracer

func TestMain(m *testing.M) {
	var err error
	var collector zipkin.Collector = zipkin.NopCollector{}
	connString := fmt.Sprintf("http://%s:%d/api/v1/spans", "localhost", 9411)
	if collector, err = zipkin.NewHTTPCollector(connString); err != nil {
		panic(err)
	}
	recorder := zipkin.NewRecorder(collector, false, "127.0.0.1", "TestFramework")
	tracer, err = zipkin.NewTracer(recorder, zipkin.ClientServerSameSpan(true), zipkin.TraceID128Bit(true))
	m.Run()
}

func initClient(t *testing.T, decls ...amqp.Declaration) amqp.Client {
	cl, err := amqp.NewClient(conn.DefaultConnector("amqp://localhost:5672"), decls...)
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func TestNewClient(t *testing.T) {
	ch := make(chan []interface{})
	store := newXStorage(1)
	go listenAndPrintlnSuff("recon", ch)
	cl := initClient(t, amqp.TemporaryExchange(testExchangeName))
	go subFunc("sub", cl, store)
	pubFunc("pub", 0, 10, cl, time.Millisecond*500, store)
	time.Sleep(time.Second * 2)
	if store.Check() {
		t.Fatal(store.Error())
	}
}

func subFunc(prefix string, client amqp.Client, storage *xStorage) {
	ch := make(chan []interface{})
	go listenAndPrintln(ch)
	s := client.Subscriber(
		amqp.SubscriberLogger(logger.NewChanLogger(ch)),
		amqp.SubscriberDeliverBefore(
			AMQPToContextFabric(tracer)("Subscribe function"),
			traceAMQPSub(tracer, "Subscribe function"),
		),
	)
	events := s.SubscribeToExchange(context.Background(), testExchangeName, x{}, amqp.Consumer{})
	for ev := range events {
		fmt.Println(prefix, "event data: ", ev.Data)
		storage.Consume(ev.Data.(*x).Num)
		ev.Done()
	}
	fmt.Println("end of events")
}

func pubFunc(prefix string, from, to int, client amqp.Client, timeout time.Duration, storage *xStorage) {
	publisher := client.Publisher(
		amqp.PublisherBefore(
			ContextToAMQP(tracer),
			traceAMQPPub(tracer, "Publish function"),
		),
	)
	fmt.Println(prefix, "start pubing")
	for s := from; s < to; s++ {
		time.Sleep(timeout)
		err := publisher.Publish(context.Background(), testExchangeName, x{s}, amqp.Publish{})
		if err != nil {
			fmt.Println(prefix, "pub error:", err)
			s--
		} else {
			storage.Pub(s)
			fmt.Println(prefix, "pub: ", s)
		}
	}
	fmt.Println(prefix, "done pubing")
}

func listenAndPrintln(ch <-chan []interface{}) {
	for e := range ch {
		fmt.Println(e...)
	}
}

const testExchangeName = "amqp-client-test"

type x struct {
	Num int
}

type xStorage struct {
	storage map[int]int
	mul     int
	m       sync.Mutex
}

func newXStorage(consumers int) *xStorage {
	return &xStorage{storage: make(map[int]int), mul: consumers}
}

func (s *xStorage) Pub(val int) {
	s.m.Lock()
	defer s.m.Unlock()
	s.storage[val] = s.storage[val] + s.mul
}

func (s *xStorage) Consume(val int) {
	s.m.Lock()
	defer s.m.Unlock()
	s.storage[val] = s.storage[val] - 1 // if it panics, then 'val' not in storage
}

func (s *xStorage) Check() bool {
	s.m.Lock()
	defer s.m.Unlock()
	for k, v := range s.storage {
		if v < 0 || v > 0 {
			fmt.Println(k, ":", v)
			return true
		}
	}
	return false
}

func (s *xStorage) Error() string {
	return fmt.Sprint(s.storage)
}

func listenAndPrintlnSuff(suff string, ch <-chan []interface{}) {
	for e := range ch {
		fmt.Println(append(e, suff)...)
	}
}

func traceAMQPPub(tracer opentracing.Tracer, operationName string) amqp.PublishingBefore {
	return func(ctx context.Context, p *amqp2.Publishing) {
		if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
			var clientSpan opentracing.Span
			if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
				clientSpan = tracer.StartSpan(
					operationName,
					opentracing.ChildOf(parentSpan.Context()),
				)
			} else {
				clientSpan = tracer.StartSpan(operationName)
			}
			defer clientSpan.Finish()
			ext.SpanKindRPCClient.Set(clientSpan)
			ctx = opentracing.ContextWithSpan(ctx, clientSpan)
		}
	}
}

func traceAMQPSub(tracer opentracing.Tracer, operationName string) amqp.DeliveryBefore {
	return func(ctx context.Context, d *amqp2.Delivery) context.Context {
		serverSpan := opentracing.SpanFromContext(ctx)
		if serverSpan == nil {
			// All we can do is create a new root span.
			serverSpan = tracer.StartSpan(operationName)
		} else {
			serverSpan.SetOperationName(operationName)
		}
		defer serverSpan.Finish()
		ext.SpanKindRPCServer.Set(serverSpan)
		ctx = opentracing.ContextWithSpan(ctx, serverSpan)
		return ctx
	}
}
