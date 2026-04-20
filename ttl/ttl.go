package ttl

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"redis-go/store"
)

type ttlEntry struct {
	key      string
	expireAt int64
	index    int //heap
}

type ttlHeap []ttlEntry

func (h ttlHeap) Len() int {
	return len(h)
}
func (h ttlHeap) Less(i int, j int) bool {
	return h[i].expireAt < h[j].expireAt
}
func (h ttlHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *ttlHeap) Push(x interface{}) {
	entry := x.(ttlEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *ttlHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1].index = -1
	*h = old[:n-1]
	return entry
}

var (
	ttlMu   sync.RWMutex
	ttlMap  = map[string]int64{}
	ttlH    = &ttlHeap{}
	onceTTL sync.Once
)

var expiredKeys int64

func ExpiredKeys() int64 {
	return atomic.LoadInt64(&expiredKeys)
}

func SetTTL(key string, sec int) {
	expireAt := time.Now().Add(time.Duration(sec) * time.Second).UnixNano()
	ttlMu.Lock()
	defer ttlMu.Unlock()
	ttlMap[key] = expireAt
	ttlEntry := ttlEntry{
		key:      key,
		expireAt: expireAt,
	}
	heap.Push(ttlH, ttlEntry)
}

func IsExpired(key string) bool {
	ttlMu.RLock()
	expireAt, exist := ttlMap[key]
	ttlMu.RUnlock()
	if !exist {
		return false
	}
	if time.Now().UnixNano() > expireAt {
		store.Store.Delete(key)
		DelTTL(key)
		atomic.AddInt64(&expiredKeys, 1)
		return true
	}
	return false
}

func DelTTL(key string) {
	ttlMu.Lock()
	defer ttlMu.Unlock()
	delete(ttlMap, key)
}

func TTL(key string) int64 {
	if _, exist := store.Store.Get(key); !exist {
		return -2
	}
	ttlMu.RLock()
	expireAt, exist := ttlMap[key]
	ttlMu.RUnlock()
	if !exist {
		return -1
	}
	left := expireAt - time.Now().UnixNano()
	if left < 0 {
		return -2
	}
	return left / 1e9
}

// 后台过期检查
func startTTLCleaner() {
	onceTTL.Do(func() {
		go func() {
			tk := time.NewTicker(100 * time.Millisecond)
			defer tk.Stop()
			for range tk.C {
				cleanExpiredSamples(20)
			}
		}()
	})
}

func cleanExpiredSamples(max int) {
	ttlMu.Lock()
	defer ttlMu.Unlock()
	now := time.Now().UnixNano()
	n := 0
	for n < max && ttlH.Len() > 0 {
		entry := (*ttlH)[0]
		if now < entry.expireAt {
			break
		}
		heap.Pop(ttlH)
		delete(ttlMap, entry.key)
		store.Store.Delete(entry.key)
		atomic.AddInt64(&expiredKeys, 1)
		n++
	}
}

func init() {
	startTTLCleaner()
}
