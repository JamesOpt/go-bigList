package biglist

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func init()  {
	rand.Seed(time.Now().UnixNano())
}

func TestBigListChain1(t *testing.T)  {
	payload := []byte("hello world")
	qc := NewQueueChains(20, 2)

	err := qc.Push(payload)
	assert.Equal(t, err, nil)
	assert.Equal(t, qc.chains.Len(), 1)

	err = qc.Push(payload)
	assert.Equal(t, err, nil)
	assert.Equal(t, qc.chains.Len(), 2)

	// fail
	err = qc.Push(payload)
	assert.Equal(t, err, ErrOverflowQueue)

	// ok
	data, err := qc.Pop()
	assert.Equal(t, err, nil)
	assert.Equal(t, data, payload)
	assert.Equal(t, qc.chains.Len(), 2)

	// ok
	qc.Pop()
	assert.Equal(t, qc.chains.Len(), 1)

	// fail
	bs, err := qc.Pop()
	assert.Equal(t, len(bs), 0)
	assert.Equal(t, qc.chains.Len(), 1)
	assert.Equal(t, err, ErrEmptyQueue)

	for i := 0; i < 5; i++ {
		err := qc.Push([]byte(strconv.Itoa(i)))
		assert.Equal(t, err, nil)
	}

	for i := 0; i < 5; i++ {
		val, err := qc.Pop()
		assert.Equal(t, err, nil)
		assert.Equal(t, val, []byte(strconv.Itoa(i)))
	}
}