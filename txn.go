package main

import (
	"redis-go/resp"
	"sync"
)

type txState uint8

const (
	txOff   txState = 0
	multiOn txState = 1
)

// ClientTx 每个连接一个事务上下文
type ClientTx struct {
	mu    sync.RWMutex
	state txState
	queue []func() resp.RespValue
}

func NewClientTx() *ClientTx {
	return &ClientTx{state: txOff}
}

// 进入事务
func (c *ClientTx) multi() resp.RespValue {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == multiOn {
		return resp.Error("ERR MULTI calls can not be nested")
	}
	c.state = multiOn
	c.queue = c.queue[:0]

	return resp.SimpleString("OK")
}

// 退出事务
func (c *ClientTx) discard() resp.RespValue {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != multiOn {
		return resp.Error("ERR DISCARD without MULTI")
	}
	c.state = txOff
	c.queue = c.queue[:0]

	return resp.SimpleString("OK")
}

// 原子执行
func (c *ClientTx) exec() resp.RespValue {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != multiOn {
		return resp.Error("ERR EXEC without MULTI")
	}

	c.state = txOff

	replies := make(resp.Array, 0, len(c.queue))
	for _, fn := range c.queue {
		replies = append(replies, fn())
	}
	c.queue = c.queue[:0]

	return replies
}

func (c *ClientTx) enqueue(fn func() resp.RespValue) resp.RespValue {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != multiOn {
		return fn()
	}

	c.queue = append(c.queue, fn)
	return resp.SimpleString("QUEUED")
}
