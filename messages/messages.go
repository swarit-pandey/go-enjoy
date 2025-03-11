package messages

import (
	"fmt"
	"sync"
	"time"
)

type Message struct {
	ID    int
	Value string
}

func NewMessages() *Message {
	return &Message{}
}

// ProcessMessage only prints message as level 0 exercise
func (m *Message) ProcessMessage(in string) {
	fmt.Println(in)
}

func (m *Message) getNMessages(n int) []Message {
	messages := []Message{}

	for i := 0; i < n; i++ {
		messages = append(messages, Message{
			ID:    i,
			Value: fmt.Sprintf("msg-%d", time.Now().UnixNano()),
		})
	}
	return messages
}

func (m *Message) ProcessMessagesConcurrently(in string, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println(in)
}

func (m *Message) Workers(id int, jobs <-chan Message, result chan<- Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		val := job.Value
		val = fmt.Sprintf("processed-%s", val)
		result <- Message{ID: id, Value: val}
	}
}

func (m *Message) WorkersWithTimeout(id int, jobs <-chan Message, result chan<- Message, wg *sync.WaitGroup) {
	defer wg.Done()

	randomTimeout := time.Duration(getRandomTimeout() * int(time.Second))

	fmt.Printf("Worker %d delaying for %d seconds\n", id, randomTimeout)
	time.Sleep(randomTimeout)

	for job := range jobs {
		val := job.Value
		val = fmt.Sprintf("processed-%s", val)
		result <- Message{ID: id, Value: val}
	}
}

func (m *Message) WorkerTimeoutAdvanced(id int, jobs <-chan Message, result chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	// Start reading the job channel and process the jobs
	for job := range jobs {
		// Get processing timeout
		processingTimeout := time.After(300 * time.Millisecond)
		done := make(chan struct{})
		var processedMsg Message

		// Simulate processing the message
		go func() {
			processingTime := time.Duration(getRandomTimeout() * int(time.Second))
			time.Sleep(processingTime)

			processedMsg = Message{
				ID:    job.ID,
				Value: fmt.Sprintf("processed-%s (took %v)", job.Value, processingTime),
			}

			done <- struct{}{}
		}()

		select {
		case <-done:
			result <- Result{Msg: processedMsg, Err: nil}
			fmt.Printf("Worker %d finished processing", id)

		case <-processingTimeout:
			result <- Result{Msg: Message{ID: job.ID, Value: ""}, Err: fmt.Errorf("Processing timeout. Worker slow")}
		}
	}
}

func (m *Message) limitedWorker(id int, jobs <-chan Message, result chan<- Message, l *Limiter, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		l.Acquire()

		val := job.Value
		val = fmt.Sprintf("processed-%s", val)
		result <- Message{ID: id, Value: val}
	}
}
