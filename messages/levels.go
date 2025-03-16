package messages

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

func getRandomTimeout() int {
	max := 10
	min := 0
	return rand.IntN(max-min) + min
}

// Take slice and print the messages as process message
func LevelZero() {
	m := NewMessages()

	messages := m.getNMessages(10)

	for _, msg := range messages {
		m.ProcessMessage(msg.Value)
	}

	fmt.Println("Level0: All messages processed")
}

func LevelOne() {
	m := NewMessages()

	messages := m.getNMessages(10)

	var wg sync.WaitGroup
	for _, msg := range messages {
		wg.Add(1)
		go m.ProcessMessagesConcurrently(msg.Value, &wg)
	}
	wg.Wait()

	fmt.Println("Level1: All messages processed")
}

func LevelTwo() {
	m := NewMessages()

	messages := m.getNMessages(100)

	// Create jobs and result channel
	jobs := make(chan Message, 100)
	results := make(chan Message)

	// Create a writer to write to jobs channel write async so processing can
	// go on async and handle channel closure here
	go func() {
		for _, message := range messages {
			jobs <- message
		}
		close(jobs)
	}()

	var wg sync.WaitGroup

	// Spawn goroutines and let them process messages
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go m.Workers(i, jobs, results, &wg)
	}

	// Handle post processing and worker finalization
	go func() {
		wg.Wait()
		close(results)
	}()

	// Create a reader to read results
	for result := range results {
		fmt.Printf("Result: {%d:%s}", result.ID, result.Value)
	}
	fmt.Println("Level2: All messages processed")
}

func LevelThree() {
	jobs := make(chan Message, 10)
	results := make(chan Message)
	done := make(chan struct{})

	globalTimeout := 2 * time.Second
	timeout := time.NewTimer(globalTimeout)
	defer timeout.Stop()

	m := NewMessages()
	messages := m.getNMessages(10)

	// Create writer to write to jobs channel
	go func() {
		for _, message := range messages {
			jobs <- message
		}
		close(jobs)
	}()

	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go m.WorkersWithTimeout(i, jobs, results, &wg)
	}

	// Wait for workers, close the results chan once they are done
	// and send a final completion signal
	go func() {
		wg.Wait()
		close(results)
		done <- struct{}{}
	}()

	// Create a reader to read result
	for {
		select {
		case result, ok := <-results:
			if !ok {
				fmt.Printf("No data in results chan yet\n")
			}
			fmt.Printf("Result: {%d:%s}\n", result.ID, result.Value)

		case <-timeout.C:
			fmt.Println("Timeout reached there, workers are slow")

		case <-done:
			fmt.Println("Level3: All messages processed")
			return
		}
	}
}

type Result struct {
	Msg Message
	Err error
}

func LevelFour() {
	// Jobs and Result chans
	jobs := make(chan Message, 10)
	results := make(chan Result)
	done := make(chan struct{})
	globalTimeout := time.After(time.Duration(10 * time.Second))

	m := NewMessages()
	messages := m.getNMessages(10)

	// Setup writers to write to jobs channel
	go func() {
		for _, message := range messages {
			jobs <- message
		}
		close(jobs)
	}()

	// Setup workers and spawn for processing
	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go m.WorkerTimeoutAdvanced(i, jobs, results, &wg)
	}

	// Setup closing it all and waiting for workers
	go func() {
		wg.Wait()
		close(results)
		done <- struct{}{}
	}()

	// Setup readers for result
	for {
		select {
		case <-done:
			fmt.Println("Level4: All messages processed")
			return

		case result := <-results:
			fmt.Printf("Result: %v\n", result)

		case <-globalTimeout:
			fmt.Println("Reached global timeout")
			return
		}
	}
}

func LevelFive() {
	startTime := time.Now()
	jobs := make(chan Message, 100)
	results := make(chan Message)

	m := NewMessages()
	messages := m.getNMessages(100)

	go func() {
		for _, message := range messages {
			jobs <- message
		}
		close(jobs)
	}()

	limiter := NewLimiter(10, 20)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go m.limitedWorker(i, jobs, results, limiter, &wg)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	processed := 0
	for result := range results {
		processed++
		elapsed := time.Since(startTime).Seconds()
		currentRate := float64(processed) / elapsed

		fmt.Printf("Result: %+v (Processed: %d, Current rate: %.2f/sec)\n",
			result, processed, currentRate)
	}

	totalTime := time.Since(startTime).Seconds()
	fmt.Printf("\nProcessed %d messages in %.2f seconds\n", processed, totalTime)
	fmt.Printf("Average rate: %.2f messages/second\n", float64(processed)/totalTime)
}

func LevelSix() {
	startTime := time.Now()
	sema := NewSemaphore(2) // 2 goroutines at once
	limiter := NewLimiter(10, 100)
	jobs := make(chan Message)
	results := make(chan Message)

	m := NewMessages()
	messages := m.getNMessages(1000)

	// Writers to write to jobs channel
	go func() {
		for _, message := range messages {
			jobs <- message
		}
		close(jobs)
	}()

	var wg sync.WaitGroup

	for i := 0; i <= 20; i++ {
		sema.Wait()
		wg.Add(1)
		go m.limitedWorker(i, jobs, results, limiter, &wg)
		sema.Signal()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Readers to read from the channel result
	processed := 0
	for result := range results {
		processed++
		elapsed := time.Since(startTime).Seconds()
		currentRate := float64(processed) / elapsed

		fmt.Printf("Result: %+v (Processed: %d, Current rate: %.2f/sec)\n",
			result, processed, currentRate)
	}

	totalTime := time.Since(startTime).Seconds()
	fmt.Printf("\nProcessed %d messages in %.2f seconds\n", processed, totalTime)
	fmt.Printf("Average rate: %.2f messages/second\n", float64(processed)/totalTime)
}

func LevelSeven() {
	m := NewMessages()
	jobsChan := make([]chan Message, 10)
	resultUpperCase := make(chan Message)
	resultReversed := make(chan Message)
	finalFormat := make(chan Message)
	resultsChan := make(chan Message)

	for i := 0; i < len(jobsChan); i++ {
		jobsChan[i] = make(chan Message)
	}

	messages := m.getNMessages(1000)

	go func() {
		for j := 0; j < 10; j++ {
			end := (j + 1) * 100
			start := end - 100
			for start < end && start < len(messages) {
				jobsChan[j] <- messages[start]
				start++
			}
		}

		for i := 0; i < 10; i++ {
			close(jobsChan[i])
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go upperCase(jobsChan[i], resultUpperCase, &wg)
	}
	go func() {
		wg.Wait()
		close(resultUpperCase)
	}()

	var reverseOrderWG sync.WaitGroup
	for i := 0; i < 3; i++ {
		reverseOrderWG.Add(1)
		go reverseOrder(resultUpperCase, resultReversed, &reverseOrderWG)
	}
	go func() {
		reverseOrderWG.Wait()
		close(resultReversed)
	}()

	var finalStringWG sync.WaitGroup
	for i := 0; i < 3; i++ {
		finalStringWG.Add(1)
		go finalString(resultReversed, finalFormat, &finalStringWG)
	}
	go func() {
		finalStringWG.Wait()
		close(finalFormat)
	}()

	var finalFormatWG sync.WaitGroup
	for i := 0; i < 3; i++ {
		finalFormatWG.Add(1)
		go fanin(finalFormat, resultsChan, &finalFormatWG)
	}
	go func() {
		finalFormatWG.Wait()
		close(resultsChan)
	}()

	for result := range resultsChan {
		fmt.Printf("Result: %+v\n", result)
	}
}

// streamMessages should basically take a value called rate, that rate will basically
// say that stream messages at this x rate. So if the value of rate is 10, then we
// send a stream of 10 messages only.
func (m *Message) streamMessages(rate int, resultChan chan<- Message, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	interval := time.Second / time.Duration(rate)
	tick := time.NewTicker(interval)
	defer tick.Stop()

	counter := 0

	for {
		select {
		case <-tick.C:
			message := Message{
				ID:    counter,
				Value: fmt.Sprintf("streamed-msg-%d", time.Now().UnixNano()),
			}
			select {
			case resultChan <- message:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func LevelEight() {
	startTime := time.Now()
	m := NewMessages()

	var wgStream sync.WaitGroup
	// Chan handling
	ctx := context.Background()
	streamMessagesChan := make(chan Message)

	wgStream.Add(1)
	go m.streamMessages(10, streamMessagesChan, ctx, &wgStream)
	go func() {
		wgStream.Wait()
		close(streamMessagesChan)
	}()

	// Start pipeline with backpressure (by using buffered channels)
	stageOneResult := make(chan Message, 2)
	stageTwoResult := make(chan Message, 2)
	stageThreeResult := make(chan Message, 2)
	finalStageResult := make(chan Message, 2)

	// A single goroutine for each stage (means we only need a snigle wait group to
	// handle this)
	var wg sync.WaitGroup

	wg.Add(4) // 4 stages for 4 goroutines
	go stageOne(streamMessagesChan, stageOneResult, ctx, &wg)
	go stageTwo(stageOneResult, stageTwoResult, ctx, &wg)
	go stageThree(stageTwoResult, stageThreeResult, ctx, &wg)
	go stageFour(stageThreeResult, finalStageResult, ctx, &wg)

	processed := 0
	for msg := range finalStageResult {
		processed++
		elapsed := time.Since(startTime).Seconds()
		currentRate := float64(processed) / elapsed

		fmt.Printf("Result: %+v (Processed: %d, Current rate: %.2f/sec)\n",
			msg, processed, currentRate)
	}
}

func LevelNine() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const maxQueueSize = 250
	const initialWorkers = 3
	const maxWorkers = 20
	const scaleCheckInterval = 2 * time.Second
	const messageStreamingRate = 100
	const workRate = 50

	queue := newQueue(maxQueueSize)
	pool := newPool(initialWorkers, maxWorkers)

	msgChan := make(chan Message)
	resultChan := make(chan Message)

	var wg sync.WaitGroup
	wg.Add(1)

	m := NewMessages()
	go m.streamMessages(messageStreamingRate, msgChan, ctx, &wg)
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Monitor queue and handle scaling
	go func() {
		ticker := time.NewTicker(scaleCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				queueDepth := queue.depth()
				queuePercent := float64(queueDepth) / float64(maxQueueSize)

				fmt.Printf("Queue status: %d/%d (%.1f%%)\n",
					queueDepth, maxQueueSize, queuePercent*100)

				if queue.shouldScaleUp() {
					added := pool.scaleUp(2)
					if added > 0 {
						fmt.Printf("Scaled up by %d workers\n", added)
					}
				}

			case <-ctx.Done():
				fmt.Printf("Context cancelled\n")
				return
			}
		}
	}()

	// Put messages to queue
	go func() {
		select {
		case msg := <-msgChan:
			fmt.Printf("Pushing message\n")
			queue.push(msg)

		case <-ctx.Done():
			fmt.Printf("Context cancelled\n")
			return
		}
	}()

	// Process messages
	go func() {
		for {
			msg, ok := queue.pop()
			if !ok {
				continue
			}

			pool.taskQueue <- func() {
				processed := m.processStringIntesive(workRate, msg)
				resultChan <- processed
			}

			select {
			case <-ctx.Done():
				fmt.Printf("Context cancelled\n")
				return
			default:
			}
		}
	}()

	// Process result
	processed := 0
	startTime := time.Now()

	for {
		select {
		case result := <-resultChan:
			processed++

			elapsed := time.Since(startTime).Seconds()
			rate := float64(processed) / elapsed

			fmt.Printf("Processed: %d, Rate: %.2f/sec\n", processed, rate)
			fmt.Printf("Result: %+v\n", result)

		case <-ctx.Done():
			fmt.Printf("Context cancelled")
			return
		}
	}
}
