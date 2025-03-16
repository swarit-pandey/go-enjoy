package messages

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

func stageOne(input <-chan Message, output chan<- Message, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(output)

	for {
		select {
		case msg, ok := <-input:
			if !ok {
				return
			}

			out := Message{
				ID:    msg.ID,
				Value: strings.ToUpper(msg.Value),
			}

			select {
			case output <- out:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func stageTwo(input <-chan Message, output chan<- Message, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(output)

	for {
		select {
		case msg, ok := <-input:
			if !ok {
				return
			}

			out := Message{
				ID:    msg.ID,
				Value: fmt.Sprintf("stage-two-%s", msg.Value),
			}

			select {
			case output <- out:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func stageThree(input <-chan Message, output chan<- Message, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(output)

	for {
		select {
		case msg, ok := <-input:
			if !ok {
				return
			}

			out := Message{
				ID:    msg.ID,
				Value: fmt.Sprintf("stage-three-%s", msg.Value),
			}

			select {
			case output <- out:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func stageFour(input <-chan Message, output chan<- Message, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(output)

	for {
		select {
		case msg, ok := <-input:
			if !ok {
				return
			}

			out := Message{
				ID:    msg.ID,
				Value: fmt.Sprintf("stage-four-%s", msg.Value),
			}

			select {
			case output <- out:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}
