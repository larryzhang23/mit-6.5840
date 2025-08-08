package mr

type Queue[T any] struct {
	items []T
}

func (q *Queue[T]) Enqueue(item T) {
	q.items = append(q.items, item)
}

func (q *Queue[T]) Dequeue() (T, bool) {
	var zero T
	if len(q.items) == 0 {
		return zero, false
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item, true
}

func (q *Queue[T]) IsEmpty() bool {
	return len(q.items) == 0
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
