package main

import (
	"log"
	"sync"
	"sync/atomic"
)

// const segmentShift = 4  16段
const segmentMask = 15

type dictEntry struct {
	key  string
	val  string
	next *dictEntry
}

type segBucket struct {
	lock sync.RWMutex
	head *dictEntry
}

type Segment struct {
	rehashIdx int64
	used      int64
	ht        [2]*segTable
}

type segTable struct {
	size    int
	mask    int
	buckets []*segBucket
}

type SegDict struct {
	segments [16]*Segment
}

func NewSegDict() *SegDict {
	d := &SegDict{}

	for i := 0; i < 16; i++ {
		seg := &Segment{
			rehashIdx: -1,
		}

		seg.ht[0] = &segTable{
			size:    4,
			mask:    3,
			buckets: make([]*segBucket, 4),
		}

		for j := 0; j < 4; j++ {
			seg.ht[0].buckets[j] = &segBucket{}
		}

		seg.ht[1] = nil

		d.segments[i] = seg
	}

	return d
}

func (s *SegDict) Add(key, val string) {
	h := murmur3Hash(key)
	seg := s.segments[h&segmentMask]

	if seg.rehashIdx != -1 {
		seg.segRehash(1)
	}

	idx := h & uint32(seg.ht[0].mask)
	bucket := seg.ht[0].buckets[idx]

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	newEntry := &dictEntry{key: key, val: val, next: bucket.head}
	bucket.head = newEntry
	seg.used++

	//是否检查负载因子
	if float64(seg.used)/float64(seg.ht[0].size) >= 1.0 {
		go seg.triggerRehash()
	}
}

func (s *SegDict) Get(key string) (string, bool) {
	h := murmur3Hash(key)
	seg := s.segments[h&segmentMask]

	if seg.rehashIdx != -1 {
		seg.segRehash(1)
	}

	idx0 := h & uint32(seg.ht[0].mask)
	bucket0 := seg.ht[0].buckets[idx0]

	bucket0.lock.RLock()
	entry := bucket0.head
	for entry != nil {
		if entry.key == key {
			return entry.val, true
		}
		entry = entry.next
	}
	bucket0.lock.RUnlock()

	if seg.ht[1] != nil {
		idx1 := h & uint32(seg.ht[1].mask)
		bucket1 := seg.ht[1].buckets[idx1]
		bucket1.lock.RLock()
		entry := bucket1.head
		for entry != nil {
			if entry.key == key {
				return entry.val, true
			}
			entry = entry.next
		}
		bucket1.lock.RUnlock()
	}

	return "", false
}

func (s *SegDict) Delete(key string) bool {
	h := murmur3Hash(key)
	seg := s.segments[h&segmentMask]

	if seg.rehashIdx != -1 {
		seg.segRehash(1)
	}

	idx0 := h & uint32(seg.ht[0].mask)
	bucket0 := seg.ht[0].buckets[idx0]
	entry := bucket0.head
	var prev *dictEntry

	bucket0.lock.Lock()
	for entry != nil {
		if entry.key == key {
			if prev == nil {
				bucket0.head = entry.next
			} else {
				prev.next = entry.next
			}
			seg.used--
			return true
		}
		prev = entry
		entry = entry.next
	}
	bucket0.lock.Unlock()

	if seg.ht[1] != nil {
		idx1 := h & uint32(seg.ht[1].mask)
		bucket1 := seg.ht[1].buckets[idx1]
		entry = bucket1.head
		prev = nil
		bucket1.lock.Lock()
		for entry != nil {
			if entry.key == key {
				if prev == nil {
					bucket1.head = entry.next
				} else {
					prev.next = entry.next
				}
				seg.used--
				return true
			}
			prev = entry
			entry = entry.next
		}
		bucket1.lock.Unlock()
	}

	return false
}

func (s *SegDict) SegScan(fn func(cmd []string)) {
	// 遍历所有 segments
	for i := 0; i < 16; i++ {
		seg := s.segments[i]

		if seg.rehashIdx != -1 {
			go seg.segRehash(int(seg.ht[0].size))
		}

		// 遍历 segment 中的每个哈希桶
		for j := 0; j < len(seg.ht[0].buckets); j++ {
			// 遍历链表中的每个条目
			seg.ht[0].buckets[j].lock.RLock()
			for entry := seg.ht[0].buckets[j].head; entry != nil; entry = entry.next {
				fn([]string{"SET", entry.key, entry.val})
			}
			seg.ht[0].buckets[j].lock.RUnlock()
		}
	}
}

func (s *Segment) shouldRehash() bool {
	if s.ht[0] == nil || s.rehashIdx != -1 {
		return false
	}

	if atomic.CompareAndSwapInt64(&s.rehashIdx, -1, 0) {
		newSize := s.ht[0].size * 2
		newTable := &segTable{
			size:    newSize,
			mask:    newSize - 1,
			buckets: make([]*segBucket, newSize),
		}
		for i := 0; i < newSize; i++ {
			newTable.buckets[i] = &segBucket{head: nil}
		}
		s.ht[1] = newTable
		s.rehashIdx = 0
		return true
	}

	return false
}

func (s *Segment) triggerRehash() {
	if s.ht[0] != nil && atomic.LoadInt64(&s.rehashIdx) == -1 {
		if s.shouldRehash() {
			go s.segRehash(s.ht[0].size)
		}
	}
}

func (s *Segment) segRehash(steps int) int {

	if s.rehashIdx == -1 || s.ht[1] == nil {
		return 0
	}

	moved := 0
	for moved < steps && s.rehashIdx < int64(s.ht[0].size) {
		//搬本段的第rehashIdx个桶
		bucket0 := s.ht[0].buckets[s.rehashIdx]
		bucket0.lock.Lock()
		entry := bucket0.head
		for entry != nil {
			next := entry.next

			h := murmur3Hash(entry.key)
			idx := h & uint32(s.ht[1].mask)

			if idx < uint32(len(s.ht[1].buckets)) && s.ht[1].buckets[idx] != nil {
				bucket1 := s.ht[1].buckets[idx]
				bucket1.lock.Lock()
				entry.next = bucket1.head
				bucket1.head = entry
				bucket1.lock.Unlock()
			}

			entry = next
			moved++
		}
		bucket0.head = nil
		s.rehashIdx++
		bucket0.lock.Unlock()
	}

	if s.rehashIdx >= int64(s.ht[0].size) {
		s.ht[0] = s.ht[1]
		s.ht[1] = nil
		s.rehashIdx = -1
		s.ht[0].size = len(s.ht[0].buckets)
		s.ht[0].mask = s.ht[0].size - 1
		log.Printf("segment rehash moved=%d,rehasIdx=%d,size=%d", moved, s.rehashIdx, s.ht[0].size)
	}

	return moved
}
