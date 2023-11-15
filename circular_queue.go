package kvdb

// CircularQueue 循环队列的实现
type CircularQueue[T any] struct {
	elements []T
	capacity uint64
	front    uint64
	back     uint64
}

func NewCircularQueue[T any](capacity uint64) *CircularQueue[T] {
	return &CircularQueue[T]{
		capacity: capacity,
		elements: make([]T, capacity),
		front:    0,
		back:     0,
	}
}

// Put 入队
func (c *CircularQueue[T]) Put(e T) {
	c.elements[c.back] = e
	c.back = (c.back + 1) % c.capacity
}

// Front 获取队头元素并移除队头
func (c *CircularQueue[T]) Front() T {
	e := c.elements[c.front]
	c.MoveFrontOneStep()
	return e
}

// Empty 判断循环队列对否为空
func (c *CircularQueue[T]) Empty() bool {
	return c.front == c.back
}

// Full 判断循环队列是否满
func (c *CircularQueue[T]) Full() bool {
	return (c.back+1)%c.capacity == c.front
}

// MoveFrontOneStep 移动front指针一次
// 使用该函数的目的主要是满足，当队列满的时候，可以选择主动覆盖
func (c *CircularQueue[T]) MoveFrontOneStep() {
	c.front = (c.front + 1) % c.capacity
}
