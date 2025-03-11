package messages

import (
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
