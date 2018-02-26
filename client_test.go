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

type XStorage struct {
	storage map[int]int
	mul     int
	m       sync.Mutex
}

func NewXStorage(consumers int) *XStorage {
	return &XStorage{storage: make(map[int]int), mul: consumers}
}

func (s *XStorage) Pub(val int) {
	s.m.Lock()
	defer s.m.Unlock()
	s.storage[val] = s.storage[val] + s.mul
}

func (s *XStorage) Consume(val int) {
	s.m.Lock()
	defer s.m.Unlock()
	s.storage[val] = s.storage[val] - 1 // if it panics, then 'val' not in storage
}

func (s *XStorage) Check() bool {
	s.m.Lock()
	defer s.m.Unlock()
	for _, v := range s.storage {
		if v < 0 || v > 0 {
			return true
		}
	}
	return false
}

func (s *XStorage) Error() string {
	return fmt.Sprint(s.storage)
}

func TestNewClient(t *testing.T) {
	ch := make(chan []interface{})
	store := NewXStorage(1)
	queuecfg := DefaultQueueConfig()
	queuecfg.AutoDelete = true
	go listenAndPrintlnSuff("recon", ch)
	cl, err := NewClient("amqp://localhost:5672", WithConnOptions(conn.WithLogger(logger.NewChanLogger(ch))), SetQueueConfig(queuecfg))
	if err != nil {
		t.Fatal(err)
	}
	go subFunc("sub", cl, store)
	pubFunc("pub", 0, 10, cl, time.Millisecond*500, store)
	time.Sleep(time.Second * 2)
	if store.Check() {
		t.Fatal(store.Error())
	}
}

func TestNewClient2(t *testing.T) {
	ch := make(chan []interface{})
	store := NewXStorage(2)
	queuecfg := DefaultQueueConfig()
	queuecfg.AutoDelete = true
	go listenAndPrintlnSuff("recon", ch)
	cl, err := NewClient("amqp://localhost:5672",
		WithOptions(
			SetMessageIdBuilder(CommonMessageIdBuilder),
		),
		WithConnOptions(
			conn.WithLogger(logger.NewChanLogger(ch)),
		),
		SetQueueConfig(queuecfg),
	)
	if err != nil {
		t.Fatal(err)
	}
	go subFunc("1", cl, store)
	go subFunc("2", cl, store)
	pubFunc("pub", 0, 10, cl, time.Millisecond*500, store)
	time.Sleep(time.Second * 2)
	if store.Check() {
		t.Fatal(store.Error())
	}
}

func TestHighLoad(t *testing.T) {
	ch := make(chan []interface{})
	store := NewXStorage(8)
	queuecfg := DefaultQueueConfig()
	queuecfg.AutoDelete = true
	go listenAndPrintlnSuff("recon", ch)
	cl1, err := NewClient("amqp://localhost:5672",
		WithOptions(
			SetMessageIdBuilder(CommonMessageIdBuilder),
		),
		WithConnOptions(
			conn.WithLogger(logger.NewChanLogger(ch)),
		),
		SetQueueConfig(queuecfg),
	)
	if err != nil {
		t.Fatal(err)
	}
	cl2, err := NewClient("amqp://localhost:5672",
		WithOptions(
			SetMessageIdBuilder(CommonMessageIdBuilder),
		),
		WithConnOptions(
			conn.WithLogger(logger.NewChanLogger(ch)),
		),
		SetQueueConfig(queuecfg),
	)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(12)
	{
		go subFunc("c1s1", cl1, store)
		go subFunc("c1s2", cl1, store)
		go subFunc("c1s3", cl1, store)
		go subFunc("c1s4", cl1, store)
		go pubFuncGroup("c1p1", 0, 10, cl1, time.Millisecond*100, &wg, store)
		go pubFuncGroup("c1p2", 100, 110, cl1, time.Millisecond*15, &wg, store)
		go pubFuncGroup("c1p3", 200, 210, cl1, time.Millisecond*654, &wg, store)
		go pubFuncGroup("c1p4", 300, 310, cl1, time.Millisecond*32, &wg, store)
		go pubFuncGroup("c1p5", 400, 410, cl1, time.Millisecond*199, &wg, store)
		go pubFuncGroup("c1p6", 500, 510, cl1, time.Millisecond*990, &wg, store)
	}
	{
		go subFunc("c2s1", cl2, store)
		go subFunc("c2s2", cl2, store)
		go subFunc("c2s3", cl2, store)
		go subFunc("c2s4", cl2, store)
		go pubFuncGroup("c2p1", 1000, 1010, cl2, time.Millisecond*1000, &wg, store)
		go pubFuncGroup("c2p2", 10100, 10110, cl2, time.Millisecond*1500, &wg, store)
		go pubFuncGroup("c2p3", 10200, 10210, cl2, time.Millisecond*1750, &wg, store)
		go pubFuncGroup("c2p4", 10300, 10310, cl2, time.Millisecond*500, &wg, store)
		go pubFuncGroup("c2p5", 10400, 10410, cl2, time.Millisecond*120, &wg, store)
		go pubFuncGroup("c2p6", 10500, 10510, cl2, time.Millisecond*3000, &wg, store)
	}
	wg.Wait()
	cl1.conn.Close()
	cl2.conn.Close()
	time.Sleep(time.Second * 2)
	if store.Check() {
		t.Fatal(store.Error())
	}
}

func subFunc(prefix string, client Client, storage *XStorage, options ...ClientConfig) {
	ch := make(chan []interface{})
	go listenAndPrintln(ch)
	events, _ := client.Sub(testExchangeName, X{}, append(options, WithOptions(AllLoggers(logger.NewChanLogger(ch))))...)
	for ev := range events {
		fmt.Println(prefix, "event data: ", ev.Data)
		storage.Consume(ev.Data.(*X).Num)
		ev.Done()
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
		fmt.Println(append(e, suff)...)
	}
}

func pubFunc(prefix string, from, to int, client Client, timeout time.Duration, storage *XStorage) {
	fmt.Println(prefix, "start pubing")
	for s := from; s < to; s++ {
		time.Sleep(timeout)
		err := client.Pub(context.Background(), testExchangeName, X{s})
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

func pubFuncGroup(prefix string, from, to int, client Client, timeout time.Duration, group *sync.WaitGroup, storage *XStorage) {
	fmt.Println(prefix, "start pubing")
	for s := from; s < to; s++ {
		time.Sleep(timeout)
		err := client.Pub(context.Background(), testExchangeName, X{s})
		if err != nil {
			fmt.Println(prefix, "pub error:", err)
			s--
		} else {
			storage.Pub(s)
			fmt.Println(prefix, "pub: ", s)
		}
	}
	fmt.Println(prefix, "done pubing")
	group.Done()
}
