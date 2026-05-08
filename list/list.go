package list

import "sync"

type listNode struct {
	value string
	prev  *listNode
	next  *listNode
}

type List struct {
	head   *listNode
	tail   *listNode
	length int
	mu     sync.RWMutex
}

func NewList() *List {
	return &List{}
}

func (l *List) LPush(value string) {
	node := &listNode{value: value}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.head == nil {
		l.head = node
		l.tail = node
	} else {
		node.next = l.head
		l.head.prev = node
		l.head = node
	}
	l.length++

}

func (l *List) RPush(value string) {
	node := &listNode{value: value}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.tail == nil {
		l.head = node
		l.tail = node
	} else {
		l.tail.next = node
		node.prev = l.tail
		l.tail = node
	}

	l.length++

}

func (l *List) LPop() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head == nil {
		return ""
	}

	val := l.head.value
	l.head = l.head.next

	if l.head != nil {
		l.head.prev = nil
	} else {
		l.tail = nil
	}

	l.length--
	return val
}

func (l *List) RPop() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.tail == nil {
		return ""
	}

	val := l.tail.value
	l.tail = l.tail.prev

	if l.tail != nil {
		l.tail.next = nil
	} else {
		l.head = nil
	}

	l.length--
	return val
}

func (l *List) LRange(start, end int) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if start < 0 {
		start = 0
	}
	if end >= l.length {
		end = l.length - 1
	}
	if start > end {
		return nil
	}

	out := make([]string, 0, end-start+1)

	var cur *listNode
	if start < l.length/2 {
		cur = l.head
		for i := 0; i < start; i++ {
			cur = cur.next
		}
	} else {
		cur = l.tail
		for i := l.length - 1; i > end; i-- {
			cur = cur.prev
		}
	}

	for i := start; i <= end; i++ {
		out = append(out, cur.value)
		cur = cur.next
	}

	return out
}

func (l *List) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.length
}
