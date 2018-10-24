package amqp

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/devimteam/amqp/conn"
	"github.com/devimteam/amqp/logger"
)

const defaultChannelIdleDuration = time.Second * 15

type (
	observer struct {
		conn         *conn.Connection
		m            sync.Mutex
		counter      chan struct{}
		idle         chan idleChan
		lastRevision time.Time
		options      observerOpts
		logger       logger.Logger
		ctx          context.Context
	}
	observerOpts struct {
		idleDuration time.Duration
		min          int
		max          int
		subLimits    struct {
			count int
			size  int
		}
	}
	ObserverOption func(opts *observerOpts)
)

// Max sets maximum amount of channels, that can be opened at the same time.
func Max(max int) ObserverOption {
	return func(opts *observerOpts) {
		opts.max = max
	}
}

// Min sets minimum amount of channels, that should be opened at the same time.
// Min does not open new channels, but forces observer not to close existing ones.
func Min(min int) ObserverOption {
	return func(opts *observerOpts) {
		opts.min = min
	}
}

// Lifetime sets duration between observer checks idle channels.
// Somewhere between dur and 2*dur observer will close channels, which do not used at least `dur` time units.
// Default value is 15 seconds.
func Lifetime(dur time.Duration) ObserverOption {
	return func(opts *observerOpts) {
		opts.idleDuration = dur
	}
}

// LimitCount limits messages for channel, by calling Qos after each reconnection.
// Pass zero for unlimited messages.
func LimitCount(count int) ObserverOption {
	return func(opts *observerOpts) {
		opts.subLimits.count = count
	}
}

// LimitSize limits messages size in bytes for channel, by calling Qos after each reconnection.
// Pass zero for unlimited messages.
func LimitSize(size int) ObserverOption {
	return func(opts *observerOpts) {
		opts.subLimits.size = size
	}
}

func newObserver(ctx context.Context, conn *conn.Connection, options ...ObserverOption) *observer {
	opts := observerOpts{
		idleDuration: defaultChannelIdleDuration,
		min:          0,
		max:          math.MaxUint16, // From https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf, section 4.9 Limitations
	}
	for _, o := range options {
		o(&opts)
	}
	pool := observer{
		conn:         conn,
		idle:         make(chan idleChan, opts.max),
		counter:      make(chan struct{}, opts.max),
		lastRevision: time.Now(),
		options:      opts,
		logger:       logger.NoopLogger,
		ctx:          ctx,
	}
	go func() {
		timer := time.NewTimer(opts.idleDuration)
		defer timer.Stop()
		for {
			// construction from time.Timer.Reset documentation
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(opts.idleDuration)
			select {
			case <-pool.ctx.Done():
				return
			case <-timer.C:
				pool.clear()
			}
		}
	}()
	return &pool
}

func (p *observer) channel() *Channel {
	p.m.Lock()
	defer p.m.Unlock()
	select {
	case idle := <-p.idle:
		return idle.ch
	default: // Go chooses case randomly, so we want to be sure, that we can choose idle channel firstly.
		select {
		case idle := <-p.idle:
			return idle.ch
		case p.counter <- struct{}{}:
			ch := Channel{
				conn:   p.conn,
				logger: p.logger,
			}
			ch.callMx.Lock() // Lock to prevent calls on nil channel. Mutex should be unlocked in `keepalive` function.
			go ch.keepalive(p.ctx, time.Minute)
			return &ch
		}
	}
}

func (p *observer) clear() {
	p.m.Lock()
	var channels []idleChan
	revisionTime := time.Now()
Loop:
	for {
		select {
		case c := <-p.idle:
			if c.ch.closed {
				continue
			}
			if revisionTime.Sub(c.since) > p.options.idleDuration && len(p.counter) < p.options.min {
				if e := c.ch.close(); e != nil {
					_ = p.logger.Log(e)
				}
				continue
			}
			channels = append(channels, c)
		default:
			break Loop
		}
	}
	for i := range channels {
		p.idle <- channels[i]
	}
	p.lastRevision = revisionTime
	p.m.Unlock()
}

func (p *observer) shouldBeClosed(revisionTime time.Time, c *idleChan) bool {
	return revisionTime.Sub(c.since) > p.options.idleDuration && len(p.counter) > p.options.min
}

func (p *observer) release(ch *Channel) {
	if ch != nil {
		p.idle <- idleChan{since: time.Now(), ch: ch}
	}
}

type idleChan struct {
	since time.Time
	ch    *Channel
}
