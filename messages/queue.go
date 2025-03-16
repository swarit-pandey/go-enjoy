package messages

type queue struct {
	buffer    chan Message
	maxSize   int
	threshold struct {
		high float64
		low  float64
	}
}

func newQueue(maxSize int) *queue {
	return &queue{
		buffer:  make(chan Message),
		maxSize: maxSize,
		threshold: struct {
			high float64
			low  float64
		}{high: 0.75, low: 0.25},
	}
}

func (q *queue) push(msg Message) bool {
	select {
	case q.buffer <- msg:
		return true
	default:
		return false
	}
}

func (q *queue) pop() (Message, bool) {
	select {
	case msg := <-q.buffer:
		return msg, true
	default:
		return Message{}, false
	}
}

func (q *queue) shouldScaleUp() bool {
	return float64(len(q.buffer))/float64(q.maxSize) >= q.threshold.high
}

func (q *queue) depth() int {
	return len(q.buffer)
}

// func (q *queue) shouldScaleDown() bool {
// 	return float64(len(q.buffer))/float64(q.maxSize) <= q.threshold.low
// }
