package main

import (
	"biglist"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
)

func randString(n int) string {
	const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}


func main()  {

	var (
		count       = 100000           // 10w
		bucketBytes = 10 * 1024 * 1024 // 100mb
		maxBuckets  = 10               // 100mb * 10
		wg          = sync.WaitGroup{}
	)
	queue := biglist.NewQueueChains(bucketBytes, maxBuckets)

	incr := 0
	lock := sync.RWMutex{}

	for i := 0; i < count ; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, err := queue.Pop()
			for err == biglist.ErrEmptyQueue {
				val, err = queue.Pop()
			}

			str := string(val)
			if strings.HasSuffix(str, "}}") && strings.HasPrefix(str, "{{") {
				lock.Lock()
				incr++
				lock.Unlock()
			}
		}()
	}

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			//length := rand.Intn(1024)
			bs := "{{" + strconv.Itoa(j) + "}}"
			queue.Push([]byte(bs))
		}(i)
	}

	wg.Wait()

	if incr != count {
		log.Panicf("counter error")
	}

	log.Println("ok")

}
