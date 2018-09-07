# AMQP
Golang AMQP wrapper is a library that wraps [amqp](https://github.com/streadway/amqp).

This lib is rethinking of [observer lib](https://github.com/devimteam/observer).

### Features
* Auto-reconnect to brocker and auto redeclare exchanges and queues.
* Control channels lifecycle: open new on high load and close unused.
* Declarative style.
    ```go
    client, err := amqp.NewClient(
    	conn.DefaultConnector("amqp://localhost:5672",
    		conn.WithLogger(lg), // We want to know connection status and errors.
    	),
    	amqp.TemporaryExchange("example-exchange"), // Declare exchanges and queues.
    	amqp.PersistentExchanges(
    		"exchange-one",
    		"exchange-two",
    		"exchange-three",
    	),
    	amqp.PersistentQueues(
    		"queue for one",
    		"queue for two",
    		"second queue for two",
    	),
    	amqp.Exchange{
    		Name: "declare directly",
    	},
    	amqp.Queue{
    		Name: "", // left empty, broker generates name for you.
    	},
    	amqp.Binding{ // do not forget to bind queue to exchange.
    		Exchange: "exchange-one",
    		Queue:    "queue for one",
    	},
    	amqp.WithLogger{Logger: lg}, // We want to know AMQP protocol errors.
    )
    ```
* Encoding and decoding hiden inside.
    * Use [Codec](https://github.com/devimteam/amqp/blob/master/codecs/codecs.go#L14) interface for your format.
    * XML, JSON and Protocol Buffers (protobuf) registered yet.
* Tons of options.
    * Min and max opened channels per publisher/subscriber.
    * Limit receiving messages.
    * Any amount of data formats.
    * Fill all message fields as you wish.
    * And more others...
* Everything from AMQP may be used directly.

## Contributing
We are waiting for your issue or pull request.
## Example
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

// Data, that we want to deal with.
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
	lg := logger.NewChanLogger(ch) // Logger interface identical to go-kit Logger.
	client, err := amqp.NewClient(
		conn.DefaultConnector("amqp://localhost:5672",
			conn.WithLogger(lg), // We want to know connection status and errors.
		),
		amqp.TemporaryExchange("example-exchange"), // Declare exchanges and queues.
		amqp.WithLogger{Logger:lg}, // We want to know AMQP protocol errors.
	)
	if err != nil {
		panic(err)
	}
	subscr := client.Subscriber()
	// context used here as closing mechanism.
	eventChan := subscr.SubscribeToExchange(context.Background(),"example-exchange", Comment{}, amqp.Consumer{})
	go func() {
		for event := range eventChan {
			fmt.Println(event.Data) // do something with events
		}
	}()
	pubsr:=client.Publisher()
	for i := 0; i < 10; i++ {
		// Prepare your data before publishing
		comment := Comment{
			Id:      strconv.Itoa(i),
			Message: "message " + strconv.Itoa(i),
		}
		// Context used here for passing data to `before` functions.
		err := pubsr.Publish(context.Background(), "example-exchange", comment, amqp.Publish{})
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Millisecond * 500)
	}
	time.Sleep(time.Second * 5) // wait for delivering all messages.
}
```
