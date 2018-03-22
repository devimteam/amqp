package amqp

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
)

// This example shows common use-case of library.
func Example_common() {
	type Comment struct {
		Id      string
		Message string
	}

	ch := make(chan []interface{})
	// Listens errors and writes them to stdout.
	go func() {
		for l := range ch {
			fmt.Println(l...)
		}
	}()
	lg := logger.NewChanLogger(ch)

	client, err := NewClient(conn.DefaultConnector(
		"amqp://localhost:5672",
		conn.WithLogger(lg), // We want to know connection status and errors.
	),
		TemporaryExchange("example-exchange"),
	)
	if err != nil {
		panic(err)
	}
	subscriber := client.Subscriber()
	eventChan := subscriber.SubscribeToExchange(context.Background(), "example-exchange", Comment{}, Consumer{})
	go func() {
		for event := range eventChan {
			fmt.Println(event.Data) // do something with events
		}
	}()
	publisher := client.Publisher()
	for i := 0; i < 10; i++ {
		// Prepare your data before publishing
		comment := Comment{
			Id:      strconv.Itoa(i),
			Message: "message " + strconv.Itoa(i),
		}
		err := publisher.Publish(context.Background(), "example-exchange", comment, Publish{})
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Millisecond * 500)
	}
	time.Sleep(time.Second * 5) // wait for delivering all events
}
