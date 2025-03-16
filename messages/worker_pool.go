package messages

import (
	"log/slog"
	"sync"
)

type worker struct {
	id           int
	taskExecuted int
}

type taskFunc func()

type pool struct {
	workers    map[int]*worker
	maxWorkers int
	taskQueue  chan taskFunc
	mu         sync.Mutex
	wg         sync.WaitGroup
}

func newPool(initialSize, maxSize int) *pool {
	p := &pool{
		workers:    make(map[int]*worker),
		maxWorkers: maxSize,
		taskQueue:  make(chan taskFunc, 100),
	}

	for i := 0; i < initialSize; i++ {
	}

	return p
}

func (p *pool) addWorker() int {
	id := len(p.workers)

	p.mu.Lock()
	w := worker{}
	p.workers[id] = &w
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		for task := range p.taskQueue {
			p.mu.Lock()
			p.workers[id].taskExecuted++
			p.mu.Unlock()

			task()
		}
	}()

	return id
}

func (p *pool) scaleUp(count int) int {
	added := 0

	for i := 0; i < count; i++ {
		p.mu.Lock()
		currentSize := len(p.workers)
		p.mu.Unlock()

		if currentSize >= p.maxWorkers {
			slog.Warn("can not scale up workers", "current_size", currentSize, "max_size", p.maxWorkers)
			break
		}

		p.addWorker()
		added++
	}

	return added
}
