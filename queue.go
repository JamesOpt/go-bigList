package biglist

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type atomicInt64 struct {
	int64
}

func newAtomicInt64(n int64) atomicInt64 {
	return atomicInt64{n}
}

func (i *atomicInt64) Add(n int64) int64 {
	return atomic.AddInt64(&i.int64, n)
}

func (i *atomicInt64) Set(n int64)  {
	atomic.StoreInt64(&i.int64, n)
}

func (i *atomicInt64) Get() int64 {
	return atomic.LoadInt64(&i.int64)
}

type BigQueueChains struct {
	sync.Mutex
	bucketBytes int
	maxBuckets int

	chains *list.List
	counter atomicInt64
}

func NewQueueChains(bucketBytes, maxBuckets int) *BigQueueChains {
	chains := list.New()
	chains.PushBack(NewBytesQueue(bucketBytes, maxBuckets, false))

	return &BigQueueChains{
		bucketBytes: bucketBytes,
		maxBuckets:  maxBuckets,
		chains:      chains,
	}
}

func (bq *BigQueueChains) incr()  {
	bq.counter.Add(1)
}

func (bq *BigQueueChains) decr()  {
	bq.counter.Add(-1)
}

func (bq *BigQueueChains) Len() int64 {
	return bq.counter.Get()
}

func (bq *BigQueueChains) allocBucket() *BytesQueue {
	return NewBytesQueue(bq.bucketBytes, bq.bucketBytes, false)
}

func (bq *BigQueueChains) Reset() {
	bq.Lock()
	defer bq.Unlock()
	for q := bq.chains.Front(); q != nil; q = q.Next() {
		q.Value.(*BytesQueue).Reset()
	}
}

func (bq *BigQueueChains) Push(data []byte) error {
	bq.Lock()
	defer bq.Unlock()

	var queue *BytesQueue
	if bq.chains.Len() == 0 {
		queue = bq.allocBucket()
	} else {
		queue = bq.chains.Back().Value.(*BytesQueue)
	}

	_, err := queue.Push(data)

	if err == nil {
		bq.incr()
		return nil
	}

	if err != ErrOverflowQueue {
		return err
	}

	if bq.chains.Len() >= bq.maxBuckets {
		return err
	}

	queue = bq.allocBucket()
	_, err = queue.Push(data)

	if err != nil {
		return err
	}

	bq.chains.PushBack(queue)
	bq.incr()
	return nil
}

func (bq *BigQueueChains) Pop() ([]byte, error) {
	bq.Lock()
	defer bq.Unlock()

	if bq.chains.Len() == 0 {
		return nil, ErrEmptyQueue
	}

	queue := bq.chains.Front().Value.(*BytesQueue) // 获取第一个元素

	if queue.Len() == 0 && bq.chains.Len() > 1 {
		bq.chains.Remove(bq.chains.Front())
		queue = bq.chains.Front().Value.(*BytesQueue) // 获取第一个元素
	}

	data, err := queue.Pop()

	if err != nil {
		return nil, err
	}

	bq.decr()

	// copy
	dst := make([]byte, len(data))
	copy(dst, data)
	return dst, err
}

func (bq *BigQueueChains) GetChains() *list.List {
	return bq.chains
}