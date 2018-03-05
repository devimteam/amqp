package amqp

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var randomSymbols = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var rander *rand.Rand

func init() {
	rander = rand.New(rand.NewSource(time.Now().Unix()))
}

var QueuePrefix = "queue-"

func genRandomQueueName() string {
	return QueuePrefix + genRandomString(15)
}

func genRandomString(length int) string {
	n := len(randomSymbols)
	b := make([]rune, length)
	for i := range b {
		b[i] = randomSymbols[rander.Intn(n)]
	}
	return string(b)
}

type stackingError struct {
	errs []string
}

func WrapError(errs ...interface{}) error {
	e := stackingError{}
	for i := range errs {
		switch err := errs[i].(type) {
		case stackingError:
			e.errs = append(e.errs, err.errs...)
		case string:
			e.errs = append(e.errs, err)
		case error:
			e.errs = append(e.errs, err.Error())
		default:
			e.errs = append(e.errs, fmt.Sprintf("%v", err))
		}
	}
	return e
}

func (e stackingError) Error() string {
	return strings.Join(e.errs, ": ")
}

type SyncedStringSlice struct {
	mx    sync.Mutex
	slice []string
}

func (s *SyncedStringSlice) Append(strs ...string) {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.slice = append(s.slice, strs...)
}

func (s *SyncedStringSlice) Get() []string {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.slice
}

func (s *SyncedStringSlice) Find(str string) int {
	s.mx.Lock()
	defer s.mx.Unlock()
	for i := range s.slice {
		if s.slice[i] == str {
			return i
		}
	}
	return -1
}

func (s *SyncedStringSlice) Drop() {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.slice = make([]string, 1)
}

type (
	matrix struct {
		core map[string]map[string]interface{}
	}
	matrixIter chan matrixVal
	matrixVal  struct {
		x, y string
		v    interface{}
	}
)

func newMatrix() (mx matrix) {
	mx.core = make(map[string]map[string]interface{})
	return
}

func (m *matrix) Get(x, y string) (interface{}, bool) {
	r, ok := m.core[x]
	if !ok {
		return nil, ok
	}
	v, ok := r[y]
	return v, ok
}

func (m *matrix) Set(x, y string, v interface{}) {
	r, ok := m.core[x]
	if ok {
		r[y] = v
		return
	}
	m.core[x] = make(map[string]interface{})
	m.core[x][y] = v
}

func (m *matrix) Iterator() matrixIter {
	iter := make(chan matrixVal)
	go func() {
		for x, r := range m.core {
			for y, v := range r {
				iter <- matrixVal{x: x, y: y, v: v}
			}
		}
		close(iter)
	}()
	return iter
}
