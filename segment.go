package main

import (
	"log"
	"sync/atomic"
	"unsafe"
)

// const segmentShift = 5  32段
const segmentMask = 31

type dictEntry struct {
	key  string
	val  string
	next *dictEntry
}

type segBucket struct {
	head unsafe.Pointer // *dictEntry
}

type Segment struct {
	rehashIdx int64
	used      int64
	ht        [2]unsafe.Pointer // *segTable
}

type segTable struct {
	size    int
	mask    int
	buckets []*segBucket
}

type SegDict struct {
	segments [32]*Segment
}

func NewSegDict() *SegDict {
	d := &SegDict{}

	for i := 0; i < 32; i++ {
		seg := &Segment{
			rehashIdx: -1,
		}

		seg.ht[0] = unsafe.Pointer(&segTable{
			size:    4,
			mask:    3,
			buckets: make([]*segBucket, 4),
		})

		for j := 0; j < 4; j++ {
			(*segTable)(seg.ht[0]).buckets[j] = &segBucket{}
		}

		seg.ht[1] = nil

		d.segments[i] = seg
	}

	return d
}

func (s *SegDict) Add(key, val string) {
	h := murmur3Hash(key)
	seg := s.segments[h&segmentMask]

	idx := h & uint32((*segTable)(seg.ht[0]).mask)
	bucket := (*segTable)(seg.ht[0]).buckets[idx]
	oldHead := (*dictEntry)(atomic.LoadPointer(&bucket.head))

	//新节点指向链表头
	newEntry := &dictEntry{
		key:  key,
		val:  val,
		next: oldHead,
	}

	atomic.StorePointer(&bucket.head, unsafe.Pointer(newEntry))
	atomic.AddInt64(&seg.used, 1)

	//是否检查负载因子
	if float64(seg.used)/float64((*segTable)(seg.ht[0]).size) >= 1.0 {
		seg.triggerRehash()
	}

	seg.segRehash()
}

func (s *SegDict) Get(key string) (string, bool) {
	h := murmur3Hash(key)
	seg := s.segments[h&segmentMask]

	idx0 := h & uint32((*segTable)(seg.ht[0]).mask)
	bucket0 := (*segTable)(seg.ht[0]).buckets[idx0]

	head1 := (*dictEntry)(atomic.LoadPointer(&bucket0.head))
	if head1 != nil {
		entry := (*dictEntry)(head1)
		for entry != nil {
			if entry.key == key {
				return entry.val, true
			}
			entry = entry.next
		}
	}

	if seg.ht[1] != nil {
		idx1 := h & uint32((*segTable)(seg.ht[1]).mask)
		bucket1 := (*segTable)(seg.ht[1]).buckets[idx1]
		head2 := atomic.LoadPointer(&bucket1.head)
		if head2 != nil {
			entry := (*dictEntry)(head2)
			for entry != nil {
				if entry.key == key {
					return entry.val, true
				}
				entry = entry.next
			}
		}
	}

	return "", false
}

func (s *SegDict) Delete(key string) bool {
	h := murmur3Hash(key)
	seg := s.segments[h&segmentMask]

	seg.segRehash()

	idx0 := h & uint32((*segTable)(seg.ht[0]).mask)
	bucket0 := (*segTable)(seg.ht[0]).buckets[idx0]

	var prev *dictEntry
	var newHead *dictEntry

	oldHead := (*dictEntry)(atomic.LoadPointer(&bucket0.head))
	entry := oldHead
	for entry != nil {
		if entry.key == key {
			if prev == nil {
				newHead = entry.next
			} else {
				prev.next = entry.next
			}
			atomic.StorePointer(&bucket0.head, unsafe.Pointer(newHead))
			atomic.AddInt64(&seg.used, -1)
			return true
		}
		prev = entry
		entry = entry.next
	}

	if seg.ht[1] != nil {
		idx1 := h & uint32((*segTable)(seg.ht[1]).mask)
		bucket1 := (*segTable)(seg.ht[1]).buckets[idx1]

		prev = nil
		newHead = nil

		oldHead1 := (*dictEntry)(atomic.LoadPointer(&bucket1.head))
		entry1 := oldHead1
		for entry1 != nil {
			if entry1.key == key {
				if prev == nil {
					newHead = entry1.next
				} else {
					prev.next = entry1.next
				}
				atomic.StorePointer(&bucket1.head, unsafe.Pointer(newHead))
				atomic.AddInt64(&seg.used, -1)
				return true
			}
			prev = entry1
			entry1 = entry1.next
		}
	}

	return false
}

func (s *SegDict) SegScan(fn func(cmd []string)) {
	// 遍历所有 segments
	for _, seg := range s.segments {
		// 遍历两个哈希表
		for tableIdx := 0; tableIdx <= 1; tableIdx++ {
			table := seg.ht[tableIdx]
			if table == nil {
				continue
			}
			// 遍历所有桶
			for _, bucket := range (*segTable)(table).buckets {
				entry := (*dictEntry)(atomic.LoadPointer(&bucket.head))
				// 遍历链表中的所有节点
				for entry != nil {
					fn([]string{"SET", entry.key, entry.val})
					entry = entry.next
				}
			}
		}
	}
}

// func (s *Segment) shouldRehash() bool {
// 	if s.ht[0] == nil || s.rehashIdx != -1 {
// 		return false
// 	}

// 	if atomic.CompareAndSwapInt64(&s.rehashIdx, -1, 0) {
// 		newSize := (*segTable)(s.ht[0]).size * 2
// 		newTable := unsafe.Pointer(&segTable{
// 			size:    newSize,
// 			mask:    newSize - 1,
// 			buckets: make([]*segBucket, newSize),
// 		})
// 		for i := 0; i < newSize; i++ {
// 			(*segTable)(newTable).buckets[i] = &segBucket{head: nil}
// 		}
// 		s.ht[1] = newTable
// 		s.rehashIdx = 0
// 		return true
// 	}

// 	return false
// }

func (s *Segment) triggerRehash() {
	// 仅当未rehash时触发（原子操作避免并发冲突）
	if atomic.CompareAndSwapInt64(&s.rehashIdx, -1, 0) {
		oldSize := (*segTable)(s.ht[0]).size
		newSize := oldSize * 2
		if newSize <= 0 {
			atomic.StoreInt64(&s.rehashIdx, -1) // 初始化失败，重置状态
			return
		}

		// 创建新表（初始化每个桶的head为nil）
		newTable := unsafe.Pointer(&segTable{
			size:    newSize,
			mask:    newSize - 1,
			buckets: make([]*segBucket, newSize),
		})
		for i := range (*segTable)(newTable).buckets {
			(*segTable)(newTable).buckets[i] = &segBucket{head: nil}
		}

		// 赋值新表（无需原子操作，因为rehash是单goroutine触发）
		s.ht[1] = newTable
		log.Printf("rehash started: oldSize=%d, newSize=%d", oldSize, newSize)
	}
}

func (s *Segment) segRehash() int {
	oldTable := s.ht[0]
	newTable := s.ht[1]
	if oldTable == nil || newTable == nil {
		return 0 // 新表未初始化，终止
	}

	moved := 0
	// 原子获取当前迁移的桶索引（初始为0，逐步递增）
	currentIdx := atomic.LoadInt64(&s.rehashIdx)
	if currentIdx >= int64((*segTable)(oldTable).size) {
		// 所有桶迁移完成：切换表并重置状态
		atomic.StorePointer(&s.ht[0], unsafe.Pointer(newTable)) // 旧表指向新表
		atomic.StorePointer(&s.ht[1], nil)                      // 新表置空
		atomic.StoreInt64(&s.rehashIdx, -1)                     // 重置rehash状态
		log.Printf("rehash completed: all %d buckets migrated", (*segTable)(oldTable).size)
		return moved
	}

	// 认领当前桶（避免并发迁移同一桶）
	if atomic.CompareAndSwapInt64(&s.rehashIdx, currentIdx, currentIdx+1) {
		oldBucket := (*segTable)(oldTable).buckets[currentIdx]
		if oldBucket == nil {
			moved++ // 空桶，计数+1
			return moved
		}

		// 迁移旧桶的所有节点到新表
		oldHead := (*dictEntry)(atomic.LoadPointer(&oldBucket.head))
		for e := oldHead; e != nil; e = e.next {
			// 计算节点在新表中的桶索引
			newIdx := murmur3Hash(e.key) & uint32((*segTable)(newTable).mask)
			newBucket := (*segTable)(newTable).buckets[newIdx]

			// 创建新节点（next指向新桶的当前head）
			newEntry := &dictEntry{
				key:  e.key,
				val:  e.val,
				next: (*dictEntry)(atomic.LoadPointer(&newBucket.head)),
			}
			// 原子替换新桶的head（保证读操作安全）
			atomic.StorePointer(&newBucket.head, unsafe.Pointer(newEntry))
		}

		// 清空旧桶（标记为已迁移）
		atomic.StorePointer(&oldBucket.head, nil)
		moved++ // 成功迁移一个桶
	}

	return moved
}
