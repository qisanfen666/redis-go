package main

// import (
// 	"redis-go/resp"
// 	"sync"
// )

// type hub struct {
// 	mu       sync.RWMutex
// 	channels map[string]map[*clientConn]struct{}
// }

// var psHub = &hub{
// 	channels: make(map[string]map[*clientConn]struct{}),
// }

// // 订阅频道
// func (h *hub) subscribe(channel string, cc *clientConn) int64 {
// 	h.mu.Lock()
// 	defer h.mu.Unlock()

// 	if h.channels[channel] == nil {
// 		h.channels[channel] = make(map[*clientConn]struct{})
// 	}

// 	_, exist := h.channels[channel][cc]
// 	if exist {
// 		return int64(len(h.channels[channel]))
// 	}

// 	h.channels[channel][cc] = struct{}{}

// 	return int64(len(h.channels[channel]))
// }

// // 退订频道
// func (h *hub) unsubscribe(channel string, cc *clientConn) int64 {
// 	h.mu.Lock()
// 	defer h.mu.Unlock()

// 	conns, ok := h.channels[channel]
// 	if !ok {
// 		return 0
// 	}
// 	_, exist := conns[cc]
// 	if !exist {
// 		return 0
// 	}
// 	delete(conns, cc)
// 	if len(conns) == 0 {
// 		delete(h.channels, channel)
// 	}
// 	return 1
// }

// // 发布消息到频道
// func (h *hub) publish(channel string, message string) int {
// 	h.mu.RLock()
// 	conns, ok := h.channels[channel]
// 	if !ok {
// 		h.mu.RUnlock() //记得解!!锁
// 		return 0
// 	}
// 	n := len(conns)
// 	list := make([]*clientConn, 0, n)
// 	for conn := range conns {
// 		list = append(list, conn)
// 	}
// 	h.mu.RUnlock()

// 	// 异步发送消息
// 	for _, conn := range list {
// 		go func(cc *clientConn) {
// 			cc.sendPubSubMessage("message", channel, message)
// 		}(conn)
// 	}

// 	return n
// }

// func (h *hub) removeAllSubscriptions(cc *clientConn) {
// 	h.mu.Lock()
// 	defer h.mu.Unlock()

// 	for channel, conns := range h.channels {
// 		if _, exist := conns[cc]; exist {
// 			delete(conns, cc)
// 			if len(conns) == 0 {
// 				delete(h.channels, channel)
// 			}
// 		}
// 	}
// }

// // 客户端发送消息
// func (cc *clientConn) sendPubSubMessage(messageType, channel, message string) {
// 	arr := resp.Array{
// 		resp.BulkString(messageType),
// 		resp.BulkString(channel),
// 		resp.BulkString(message),
// 	}
// 	cc.mu.Lock()
// 	cc.w.Write(arr.ToBytes())
// 	cc.w.Flush()
// 	cc.mu.Unlock()
// }
