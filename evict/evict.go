package evict

import (
	"math/rand/v2"
	"redis-go/store"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Policy byte

const (
	PolicyNone Policy = iota
	PolicyAllKeysLRU
	PolicyAllKeysLFU
	PolicyVolatileLRU
	PolicyVolatileLFU
)

var Config = struct {
	MaxMemory   int64
	Policy      Policy
	SampleCount int
}{
	MaxMemory:   0,
	Policy:      PolicyNone,
	SampleCount: 5,
}

type meta struct {
	lastAccess int64  // LRU时间戳
	freq       uint16 // LFU次数
}

var (
	metaMu sync.RWMutex
	metaDB = map[string]*meta{} // key -> meta
)

func Touch(key string) {
	metaMu.Lock()
	defer metaMu.Unlock()

	m, exist := metaDB[key]
	if !exist {
		metaDB[key] = &meta{
			lastAccess: time.Now().Unix(),
			freq:       1,
		}
		return
	}
	m.lastAccess = time.Now().Unix()
	m.freq++
}

func DeleteMeta(key string) {
	metaMu.Lock()
	defer metaMu.Unlock()

	delete(metaDB, key)
}

// 估算内存使用
func EstimateMemoryUsage(key, val string) int64 {
	return int64(len(key) + len(val) + 48 + 16)
}

func randomKeys(n int) []string {
	metaMu.RLock()
	defer metaMu.RUnlock()

	total := len(metaDB)
	if total == 0 {
		return []string{}
	}
	if n > total {
		n = total
	}

	keys := make([]string, 0, total)
	for k := range metaDB {
		keys = append(keys, k)
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return keys[:n]
}

/*------LRU------*/
type lruItem struct {
	key  string
	time int64
}

func evictLRU(count int) {
	keys := randomKeys(Config.SampleCount)
	if len(keys) == 0 {
		return
	}
	items := make([]lruItem, 0, Config.SampleCount)

	metaMu.RLock()
	for _, key := range keys {
		if m, ok := metaDB[key]; ok {
			items = append(items, lruItem{
				key:  key,
				time: m.lastAccess,
			})
		}
	}
	metaMu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].time < items[j].time
	})

	for i := 0; i < count && i < len(items); i++ {
		store.Store.Delete(items[i].key)
		DeleteMeta(items[i].key)
	}
}

/*------LFU------*/
type lfuItem struct {
	key  string
	freq uint16
}

func evictLFU(count int) {
	keys := randomKeys(Config.SampleCount)
	if len(keys) == 0 {
		return
	}
	items := make([]lfuItem, 0, count)
	metaMu.RLock()
	for _, key := range keys {
		if m, ok := metaDB[key]; ok {
			items = append(items, lfuItem{
				key:  key,
				freq: m.freq,
			})
		}
	}
	metaMu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].freq < items[j].freq
	})

	for i := 0; i < count && i < len(items); i++ {
		store.Store.Delete(items[i].key)
		DeleteMeta(items[i].key)
	}
}

var usedMemory int64

func GetMemoryUsage() int64 {
	return atomic.LoadInt64(&usedMemory)
}

func UpdateMemoryUsage(delta int64) {
	atomic.AddInt64(&usedMemory, delta)
}

// 淘汰n个key
func Evict(n int) {
	switch Config.Policy {
	case PolicyAllKeysLRU:
		evictLRU(n)
	case PolicyAllKeysLFU:
		evictLFU(n)
	}
}

func MaybeEvict() bool {
	if Config.MaxMemory <= 0 {
		return false
	}
	for GetMemoryUsage() > Config.MaxMemory {
		Evict(1)
		return true
	}
	return false
}
