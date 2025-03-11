package messages

type Semaphore struct {
	count chan struct{} // Track the count of semaphores
}

// NewSemaphore takes the number of workers that can enter in the critical
// section to do operation
func NewSemaphore(num int) *Semaphore {
	s := &Semaphore{
		count: make(chan struct{}, num),
	}

	for i := 0; i < num; i++ {
		s.count <- struct{}{}
	}

	return s
}

func (s *Semaphore) Wait() {
	<-s.count // Block until we can get a token
}

func (s *Semaphore) Signal() {
	s.count <- struct{}{} // Release the token
}
