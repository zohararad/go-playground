package main

// This is a rudimentary example of a token-bucket rate-limiter.
// The bucket has a maximum capacity, and a pre-determined rate at which capacity is replenished (for example 10 requests every 1 minute).
// Incoming requests are processed as long as the bucket has available tokens, and are rejected when the bucket's capacity reaches zero.

import (
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"
)

// bucket defines a token bucket that replenishes its capacity at a known rate
type bucket struct {
	capacity      int
	done          chan bool
	lock          sync.Mutex
	queue         chan int8
	replenishSize int
	ticker        *time.Ticker
}

// newBucket creates a new token bucket. it takes the bucket's maximum capacity, replenish rate and size
// and returns a new instance.
// capacity determines how big the bucket is (e.g. 100 tokens)
// rate determines the capacity replenish frequency (e.g. every 5 seconds)
// replenishSize determines how many tokens are added each time the bucket's capacity is replenished
func newBucket(capacity int, rate time.Duration, replenishSize int) *bucket {
	b := &bucket{
		capacity:      capacity,
		done:          make(chan bool),
		queue:         make(chan int8, capacity),
		replenishSize: replenishSize,
		ticker:        time.NewTicker(rate),
	}
	return b
}

// start sets up the bucket's initial capacity and then runs the periodical replenishing function
func (b *bucket) start() {
	b.replenish(b.capacity)
	go b.fillBucketOrStop()
}

// stop signals the periodical replenishing function to stop running thus terminating the bucket's operation
func (b *bucket) stop() {
	b.done <- true
}

// fillBucketOrStop runs in a goroutine and either calls the bucket replenish function when the internal
// ticker is fired, or quits when stop() is called
func (b *bucket) fillBucketOrStop() {
	for {
		select {
		case <-b.ticker.C:
			b.replenish(b.replenishSize)
			break
		case <-b.done:
			return
		}
	}
}

// replenish takes a maximum number of tokens and fills up the bucket's internal storage with new tokens
// either up to requested amount or until the bucket is full.
func (b *bucket) replenish(upTo int) {
	log.Println("replenish queue start")
	b.lock.Lock()
	defer b.lock.Unlock()
	if len(b.queue) == b.capacity {
		log.Println("queue at maximum capacity. cannot replenish")
		return
	}
	for n := 0; n < upTo; n++ {
		b.queue <- 1
		if len(b.queue) == b.capacity {
			break
		}
		log.Printf("queue at capacity of %d of %d", len(b.queue), b.capacity)
	}
	log.Println("replenish queue finish")
}

// take tries to take an available token from the bucket. if a token is available,
// it returns 1. otherwise it returns 0
func (b *bucket) take() int8 {
	log.Println("take from queue start")
	b.lock.Lock()
	defer b.lock.Unlock()
	var ret int8 = 0
	if len(b.queue) == 0 {
		ret = 0
	} else {
		ret = <-b.queue
	}
	log.Printf("take from queue: %d", ret)
	return ret
}

// runWorker is called from main() and executes a simulation of this code
// go ahead and change the parameters passed to the newBucket call to see how the code behaves
func runWorker() {
	rndm := rand.New(rand.NewSource(time.Now().UnixNano()))
	bkt := newBucket(30, 3*time.Second, 10)
	bkt.start()
	defer bkt.stop()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill)
mainLoop:
	for {
		select {
		case <-sig:
			log.Println("quiting!")
			break mainLoop
		default:
			if bkt.take() == 1 {
				log.Println("I can run! Sleeping a bit")
				n := rndm.Intn(1000)
				time.Sleep(time.Duration(n) * time.Millisecond)
			} else {
				log.Println("I can't run!")
			}
		}
	}
}

func main() {
	runWorker()
}
