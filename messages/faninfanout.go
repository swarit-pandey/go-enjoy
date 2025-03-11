package messages

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

func upperCase(jobs <-chan Message, resultUpperCase chan<- Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		resultUpperCase <- Message{ID: job.ID, Value: strings.ToUpper(job.Value)}
	}
}

func reverseOrder(jobs <-chan Message, resultReverseOrder chan<- Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		reverse := func(in string) string {
			runes := []rune(in)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes)
		}

		reversed := reverse(job.Value)

		resultReverseOrder <- Message{ID: job.ID, Value: reversed}
	}
}

func finalString(jobs <-chan Message, finalResult chan<- Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		finalString := fmt.Sprintf("%s-%d", job.Value, time.Now().UnixNano())

		finalResult <- Message{ID: job.ID, Value: finalString}
	}
}

func fanin(jobs <-chan Message, final chan<- Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		final <- job
	}
}
