package amqp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
)

const testExchangeName = "amqp-client-test"

type X struct {
	Num int
}

func TestNewClient(t *testing.T) {
	ch := make(chan []interface{})
	go listenAndPrintlnSuff("recon", ch)
	cl, err := NewClient("amqp://localhost:5672", WithConnOptions(conn.WithLogger(logger.NewChanLogger(ch))))
	if err != nil {
		t.Fatal(err)
	}
	go subFunc("sub", cl)
	pubFunc("pub", 0, 10, cl, time.Millisecond*500)
	time.Sleep(time.Second * 2)
}

func TestNewClient2(t *testing.T) {
	ch := make(chan []interface{})
	go listenAndPrintlnSuff("recon", ch)
	cl, err := NewClient("amqp://localhost:5672",
		WithOptions(
			SetMessageIdBuilder(CommonMessageIdBuilder),
		),
		WithConnOptions(
			conn.WithLogger(logger.NewChanLogger(ch)),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	go subFunc("1", cl)
	go subFunc("2", cl)
	pubFunc("pub", 0, 10, cl, time.Millisecond*500)
	time.Sleep(time.Second * 2)
}

func TestHighLoad(t *testing.T) {
	ch := make(chan []interface{})
	cancel1 := make(chan conn.Signal)
	go listenAndPrintlnSuff("recon", ch)
	cl1, err := NewClient("amqp://localhost:5672",
		WithOptions(
			SetMessageIdBuilder(CommonMessageIdBuilder),
		),
		WithConnOptions(
			conn.WithLogger(logger.NewChanLogger(ch)),
			conn.WithCancel(cancel1),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	cancel2 := make(chan conn.Signal)
	cl2, err := NewClient("amqp://localhost:5672",
		WithOptions(
			SetMessageIdBuilder(CommonMessageIdBuilder),
		),
		WithConnOptions(
			conn.WithLogger(logger.NewChanLogger(ch)),
			conn.WithCancel(cancel2),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(12)
	{
		go subFunc("c1s1", cl1)
		go subFunc("c1s2", cl1)
		go subFunc("c1s3", cl1)
		go subFunc("c1s4", cl1)
		go pubFuncGroup("c1p1", 0, 10, cl1, time.Millisecond*100, &wg)
		go pubFuncGroup("c1p2", 100, 110, cl1, time.Millisecond*15, &wg)
		go pubFuncGroup("c1p3", 200, 210, cl1, time.Millisecond*654, &wg)
		go pubFuncGroup("c1p4", 300, 310, cl1, time.Millisecond*32, &wg)
		go pubFuncGroup("c1p5", 400, 410, cl1, time.Millisecond*199, &wg)
		go pubFuncGroup("c1p6", 500, 510, cl1, time.Millisecond*990, &wg)
	}
	{
		go subFunc("c2s1", cl2)
		go subFunc("c2s2", cl2)
		go subFunc("c2s3", cl2)
		go subFunc("c2s4", cl2)
		go pubFuncGroup("c2p1", 1000, 1010, cl2, time.Millisecond*1000, &wg)
		go pubFuncGroup("c2p2", 10100, 10110, cl2, time.Millisecond*1500, &wg)
		go pubFuncGroup("c2p3", 10200, 10210, cl2, time.Millisecond*1750, &wg)
		go pubFuncGroup("c2p4", 10300, 10310, cl2, time.Millisecond*500, &wg)
		go pubFuncGroup("c2p5", 10400, 10410, cl2, time.Millisecond*120, &wg)
		go pubFuncGroup("c2p6", 10500, 10510, cl2, time.Millisecond*3000, &wg)
	}
	wg.Wait()
	cancel1 <- conn.Signal{}
	cancel2 <- conn.Signal{}
	time.Sleep(time.Second * 2)
}

func subFunc(prefix string, client Client, options ...ClientOption) {
	ch := make(chan []interface{})
	go listenAndPrintln(ch)
	events, _ := client.Sub(testExchangeName, X{}, append(options, WithOptions(AllLoggers(logger.NewChanLogger(ch))))...)
	for ev := range events {
		fmt.Println(prefix, "event data: ", ev.Data)
		ev.Commit()
	}
	fmt.Println("end of events")
}

func listenAndPrintln(ch <-chan []interface{}) {
	for e := range ch {
		fmt.Println(e...)
	}
}

func listenAndPrintlnSuff(suff string, ch <-chan []interface{}) {
	for e := range ch {
		fmt.Println(append(e, ch)...)
	}
}

func pubFunc(prefix string, from, to int, client Client, timeout time.Duration) {
	fmt.Println(prefix, "start pubing")
	for s := from; s < to; s++ {
		err := client.Pub(context.Background(), testExchangeName, X{s})
		if err != nil {
			fmt.Println(prefix, "pub error:", err)
			s--
		} else {
			fmt.Println(prefix, "pub: ", s)
		}
		time.Sleep(timeout)
	}
	fmt.Println(prefix, "done pubing")
}

func pubFuncGroup(prefix string, from, to int, client Client, timeout time.Duration, group *sync.WaitGroup) {
	fmt.Println(prefix, "start pubing")
	for s := from; s < to; s++ {
		err := client.Pub(context.Background(), testExchangeName, X{s})
		if err != nil {
			fmt.Println(prefix, "pub error:", err)
			s--
		} else {
			fmt.Println(prefix, "pub: ", s)
		}
		time.Sleep(timeout)
	}
	fmt.Println(prefix, "done pubing")
	group.Done()
}
