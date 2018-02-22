package amqp

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var randomSymbols = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func init() {
	rand.Seed(time.Now().Unix())
}

var QueuePrefix = "queue-"

func genRandomQueueName() string {
	return QueuePrefix + genRandomString(15)
}

func genRandomString(length int) string {
	n := len(randomSymbols)
	b := make([]rune, length)
	for i := range b {
		b[i] = randomSymbols[rand.Intn(n)]
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
