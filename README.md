# AMQP
Golang AMQP wrapper is a library that wraps [amqp](https://github.com/streadway/amqp).

This lib is rethinking of [observer lib](https://github.com/devimteam/observer).

```go
package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/devimteam/amqp"
	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
)

type Comment struct {
	Id      string
	Message string
}

func main() {
	ch := make(chan []interface{})
	// Listens errors and writes them to stdout.
	go func() {
		for l := range ch {
			fmt.Println(l...)
		}
	}()
	lg := logger.NewChanLogger(ch)
	queuecfg := amqp.DefaultQueueConfig()
	queuecfg.AutoDelete = true
	client, err := amqp.NewClient("amqp://localhost:5672",
		amqp.WithOptions(
			amqp.WaitConnection(true, time.Minute),
			amqp.ApplicationId("example app"),
			amqp.WarnLogger(lg),
			amqp.ErrorLogger(lg),
		),
		amqp.WithConnOptions(
			conn.WithLogger(lg), // We want to know connection status and errors.
		),
		amqp.SetQueueConfig(queuecfg),
	)
	if err != nil {
		panic(err)
	}
	eventChan, _ := client.Sub("example-exchange", Comment{})
	go func() {
		for event := range eventChan {
			fmt.Println(event.Data) // do something with events
		}
	}()
	for i := 0; i < 10; i++ {
		// Prepare your data before publishing
		comment := Comment{
			Id:      strconv.Itoa(i),
			Message: "message " + strconv.Itoa(i),
		}
		err := client.Pub(context.Background(), "example-exchange", comment)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Millisecond * 500)
	}
	time.Sleep(time.Second * 5) // wait for delivering all events
}
```
