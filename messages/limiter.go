package messages

import (
	"time"
)

type Limiter struct {
	token chan struct{} // Bucket for token
	rate  int           // Tokens per second
	burst int           // Maximum burst capacity
}

func NewLimiter(rate, burst int) *Limiter {
	l := &Limiter{
		token: make(chan struct{}, burst),
		rate:  rate,
		burst: burst,
	}

	for i := 0; i < burst; i++ {
		l.token <- struct{}{}
	}

	go l.refillTokens()
	return l
}

func (l *Limiter) refillTokens() {
	tick := time.NewTicker(time.Second / time.Duration(l.rate))
	defer tick.Stop()

	for range tick.C {
		select {
		case l.token <- struct{}{}:
			// Added a new token
		default:
			// Bucket is full
		}
	}
}

func (l *Limiter) Acquire() {
	// Block
	<-l.token
}
