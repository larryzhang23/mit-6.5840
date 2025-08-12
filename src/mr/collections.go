package mr

import "sync"

type Queue[T any] struct {
	mu sync.Mutex
	items []T
}

func (q *Queue[T]) Enqueue(item T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, item)
}

func (q *Queue[T]) Dequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var zero T
	if len(q.items) == 0 {
		return zero, false
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item, true
}

func (q *Queue[T]) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) == 0
}

func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// import "container/list"

// type Queue[T any] struct {
// 	list *list.List
// }

// func NewQueue[T any]() *Queue[T] {
// 	return &Queue[T]{list: list.New()}
// }

// func (q *Queue[T]) Enqueue(value T) {
// 	q.list.PushBack(value)
// }

// func (q *Queue[T]) Dequeue() (T, bool) {
// 	var zero T
// 	front := q.list.Front()
// 	if front == nil {
// 		return zero, false
// 	}
// 	q.list.Remove(front)
// 	return front.Value.(T), true
// }

// func (q *Queue[T]) IsEmpty() bool {
// 	return q.list.Len() == 0
// }


type MapChan[T comparable] struct {
	mu sync.Mutex
	mapping map[T]chan bool
}

func NewMapChan[T comparable]() *MapChan[T] {
	return &MapChan[T]{
		mapping: make(map[T]chan bool),
	}
}

func (m *MapChan[T]) NewChan(k T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mapping[k] = make(chan bool, 1)
}

func (m *MapChan[T]) GetChan(k T) chan bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mapping[k]
}

func (m *MapChan[T]) DeleteChan(k T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mapping, k)
}

func (m *MapChan[T]) SendSignal(k T) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.mapping[k]; 
	if ok {
		// non-blocking
		ch <- true
	} 
	return ok
}


type FileStore[T comparable] struct {
	mu sync.Mutex
	mapping map[T][]string
	totalFiles int 
	actualFiles int

}

func NewMapString[T comparable](nsplits int, nreduce int) *FileStore[T] {
	return &FileStore[T]{
		mapping: make(map[T][]string),
		totalFiles: nsplits * nreduce,
	}
}

func (m *FileStore[T]) GetFiles(k T) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mapping[k]
}

func (m *FileStore[T]) AddFile(k T, v string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mapping[k] = append(m.mapping[k], v)
	m.actualFiles++
}

func (m *FileStore[T]) IsAllFiles() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.actualFiles == m.totalFiles
}

