package utils

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	lock    = sync.Mutex{}
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey get formated key, for test only
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("kvdb-test-key-%09d", i))
}

// RandomValue generate random value, for test only
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		lock.Lock()
		b[i] = letters[randStr.Intn(len(letters))]
		lock.Unlock()
	}
	return []byte("kvdb-test-value-" + string(b))
}
