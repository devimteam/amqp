package conn

import (
	"fmt"
	"testing"
	"time"

	"github.com/devimteam/amqp/logger"
)

// Tests 3 cases
// 1: Test reconnection mechanism itself.
// 2: Test NotifyClose, when RabbitMQ is UP.
// 3: Test immediate return from NotifyClose, when RabbitMQ is DOWN.
func TestDial(t *testing.T) {
	ch := make(chan []interface{})
	go listenAndPrintln(ch)
	var args = struct {
		url  string
		opts []ConnectionOption
	}{url: "amqp://localhost:5672", opts: []ConnectionOption{WithLogger(logger.NewChanLogger(ch))}}
	conn, _ := Dial(args.url, args.opts...)
	time.Sleep(time.Second * 20)
	fmt.Println("Test: wait closing")
	<-conn.NotifyClose()
	fmt.Println("Test: Closed")
	time.Sleep(time.Second)
}

// Test Attempts option.
func TestDialNTimes(t *testing.T) {
	ch := make(chan []interface{})
	var args = struct {
		url  string
		opts []ConnectionOption
	}{url: "amqp://localhost:5672", opts: []ConnectionOption{WithLogger(logger.NewChanLogger(ch)), Attempts(3)}}
	go Dial(args.url, args.opts...)
	waitFor(MaxAttemptsError, ch)
}

// Test WithCancel option.
func TestDialCancel(t *testing.T) {
	ch := make(chan []interface{})
	cancel := make(chan Signal)
	go waitFor(CanceledError, ch)
	var args = struct {
		url  string
		opts []ConnectionOption
	}{url: "amqp://localhost:5672", opts: []ConnectionOption{WithLogger(logger.NewChanLogger(ch)), WithCancel(cancel)}}
	go Dial(args.url, args.opts...)
	time.Sleep(time.Second * 10)
	cancel <- Signal{}
	time.Sleep(time.Second * 3)
}

// Test WithDelay option.
func TestDialDelay(t *testing.T) {
	ch := make(chan []interface{})
	var args = struct {
		url  string
		opts []ConnectionOption
	}{url: "amqp://localhost:5672", opts: []ConnectionOption{
		WithLogger(logger.NewChanLogger(ch)),
		Attempts(10),
		WithDelay(time.Millisecond*100, time.Millisecond*100),
	}}
	go Dial(args.url, args.opts...)
	waitFor(MaxAttemptsError, ch)
}

// Test WithDelayBuilder option.
func TestDialDelayBuilder(t *testing.T) {
	ch := make(chan []interface{})
	var args = struct {
		url  string
		opts []ConnectionOption
	}{url: "amqp://localhost:5672", opts: []ConnectionOption{
		WithLogger(logger.NewChanLogger(ch)),
		Attempts(30),
		WithDelayBuilder(testDelayBuilder),
	}}
	go Dial(args.url, args.opts...)
	waitFor(MaxAttemptsError, ch)
}

func waitFor(v interface{}, ch <-chan []interface{}) {
	for e := range ch {
		fmt.Println(e...)
		for i := range e {
			if v == e[i] {
				return
			}
		}
	}
}

func listenAndPrintln(ch <-chan []interface{}) {
	for e := range ch {
		fmt.Println(e...)
	}
}
func testDelayBuilder() Timeouter {
	return testDelayer{}
}

type testDelayer struct{}

func (testDelayer) Wait() { time.Sleep(time.Millisecond * 20) }
func (testDelayer) Inc()  {}
