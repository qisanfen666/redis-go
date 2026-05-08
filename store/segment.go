package store

import (
	"fmt"
	"redis-go/hash"
	"redis-go/list"
	"redis-go/zset"
	"sync"
	"sync/atomic"
)

var Store = NewSegDict()

type SegmentDict struct {
	segments [32]*Segment
	Zsets    map[string]*(zset.Zset)
	Lists    map[string]*(list.List)
	zm       sync.RWMutex
}

type Segment struct {
	mu     sync.RWMutex
	table0 *HashTable
	table1 *HashTable

	rehashing bool
	rehashIdx int64
}

type HashTable struct {
	size    int64
	mask    int64
	buckets []*Bucket
}

type Bucket struct {
	head *Entry
}

type Entry struct {
	key  string
	val  string
	next *Entry
}

func NewSegDict() *SegmentDict {
	d := &SegmentDict{
		Zsets: make(map[string]*(zset.Zset)),
		Lists: make(map[string]*(list.List)),
	}

	for i := 0; i < 32; i++ {
		d.segments[i] = &Segment{
			table0: NewHashTable(4),
		}
	}

	return d
}

func NewHashTable(size int64) *HashTable {
	if size <= 0 {
		size = 4
	}

	ht := &HashTable{
		size:    size,
		mask:    size - 1,
		buckets: make([]*Bucket, size),
	}

	for i := int64(0); i < size; i++ {
		ht.buckets[i] = &Bucket{}
	}

	return ht
}

func (sd *SegmentDict) getSegment(key string) *Segment {
	h := hash.Murmur3Hash(key)
	segmentIdx := h & 31
	return sd.segments[segmentIdx]
}

func (sd *SegmentDict) Set(key, val string) {
	seg := sd.getSegment(key)
	seg.mu.Lock()
	defer seg.mu.Unlock()

	seg.tryRehash(1)

	h := hash.Murmur3Hash(key)

	var targetTable *HashTable
	var targetIdx int64

	if seg.rehashing {
		targetTable = seg.table1
		targetIdx = int64(h) & seg.table1.mask

		oldIdx := int64(h) & seg.table0.mask
		seg.table0.remove(key, oldIdx)
	} else {
		targetTable = seg.table0
		targetIdx = int64(h) & seg.table0.mask
	}

	bucket := targetTable.buckets[targetIdx]

	//遍历bucket
	entry := bucket.head

	for entry != nil {
		if entry.key == key {
			entry.val = val
			return
		}
		entry = entry.next
	}

	newEntry := &Entry{
		key:  key,
		val:  val,
		next: bucket.head,
	}
	bucket.head = newEntry

	//更新计数
	if targetTable == seg.table0 {
		atomic.AddInt64(&seg.table0.size, 1)
	}

	//检查是否开始rehash
	if !seg.rehashing && seg.shouldStartRehash() {
		seg.startRehash()
	}
}

func (sd *SegmentDict) Get(key string) (string, bool) {
	seg := sd.getSegment(key)
	seg.mu.RLock()
	defer seg.mu.RUnlock()

	h := hash.Murmur3Hash(key)

	if seg.rehashing {
		if val, found := seg.table0.get(key, int64(h)&seg.table0.mask); found {
			return val, true
		}

		return seg.table1.get(key, int64(h)&seg.table1.mask)
	}

	return seg.table0.get(key, int64(h)&seg.table0.mask)
}

func (sd *SegmentDict) Delete(key string) bool {
	seg := sd.getSegment(key)
	seg.mu.Lock()
	defer seg.mu.Unlock()

	seg.tryRehash(1)

	h := hash.Murmur3Hash(key)
	deleted := false

	//从table0里删除
	idx0 := int64(h) & seg.table0.mask
	if seg.table0.remove(key, idx0) {
		deleted = true
	}

	if seg.rehashing {
		idx1 := int64(h) & seg.table1.mask
		if seg.table1.remove(key, idx1) {
			deleted = true
		}
	}

	return deleted
}

func (s *Segment) shouldStartRehash() bool {
	loadFactor := float64(atomic.LoadInt64(&s.table0.size)) / float64(s.table0.size)
	return loadFactor > 1.0
}

func (s *Segment) startRehash() {
	if s.rehashing {
		return
	}

	newSize := s.table0.size * 2
	if newSize <= 0 {
		newSize = 8
	}

	s.table1 = NewHashTable(newSize)
	s.rehashing = true
	s.rehashIdx = 0
}

func (s *Segment) tryRehash(n int) {
	if !s.rehashing {
		return
	}

	for i := 0; i < n && s.rehashIdx < s.table0.size; i++ {
		//迁移一个桶
		bucket := s.table0.buckets[s.rehashIdx]
		if bucket.head != nil {
			entry := bucket.head
			for entry != nil {
				newIdx := hash.Murmur3Hash(entry.key) & uint32(s.table1.mask)
				newBucket := s.table1.buckets[newIdx]

				newEntry := &Entry{
					key:  entry.key,
					val:  entry.val,
					next: newBucket.head,
				}
				newBucket.head = newEntry

				entry = entry.next
			}

			bucket.head = nil
		}

		s.rehashIdx++
	}

	if s.rehashIdx >= s.table0.size {
		s.table0 = s.table1
		s.table1 = nil
		s.rehashing = false
		s.rehashIdx = 0
	}
}

func (ht *HashTable) get(key string, idx int64) (string, bool) {
	bucket := ht.buckets[idx]
	entry := bucket.head

	for entry != nil {
		if entry.key == key {
			return entry.val, true
		}
		entry = entry.next
	}

	return "", false
}

func (ht *HashTable) remove(key string, idx int64) bool {
	bucket := ht.buckets[idx]
	if bucket.head == nil {
		return false
	}

	if bucket.head.key == key {
		bucket.head = bucket.head.next
		atomic.AddInt64(&ht.size, -1)
		return true
	}

	prev := bucket.head
	cur := prev.next

	for cur != nil {
		if cur.key == key {
			prev.next = cur.next
			atomic.AddInt64(&ht.size, -1)
			return true
		}
		prev = cur
		cur = cur.next
	}

	return false
}

func (sd *SegmentDict) Scan(fn func(key, value string)) {
	for _, seg := range sd.segments {
		seg.mu.RLock()

		for _, bucket := range seg.table0.buckets {
			entry := bucket.head
			for entry != nil {
				fn(entry.key, entry.val)
				entry = entry.next
			}
		}

		if seg.rehashing {
			for _, bucket := range seg.table1.buckets {
				entry := bucket.head
				for entry != nil {
					fn(entry.key, entry.val)
					entry = entry.next
				}
			}
		}

		seg.mu.RUnlock()
	}
}

func (sd *SegmentDict) Stats() map[string]interface{} {
	stats := make(map[string]interface{})
	totalEntries := 0
	rehashingSegments := 0

	for i, seg := range sd.segments {
		seg.mu.RLock()
		segmentStats := make(map[string]interface{})
		segmentStats["table0_size"] = seg.table0.size
		segmentStats["table0_buckets"] = len(seg.table0.buckets)

		count0 := 0
		for _, bucket := range seg.table0.buckets {
			entry := bucket.head
			for entry != nil {
				count0++
				entry = entry.next
			}
		}
		segmentStats["table0_entries"] = count0

		if seg.rehashing {
			rehashingSegments++
			segmentStats["rehashing"] = true
			segmentStats["rehash_idx"] = seg.rehashIdx
			segmentStats["table1_size"] = seg.table1.size
			segmentStats["table1_buckets"] = len(seg.table1.buckets)

			// 计算table1中的条目数
			count1 := 0
			for _, bucket := range seg.table1.buckets {
				entry := bucket.head
				for entry != nil {
					count1++
					entry = entry.next
				}
			}
			segmentStats["table1_entries"] = count1
		} else {
			segmentStats["rehashing"] = false
		}

		totalEntries += count0
		if seg.rehashing {
			totalEntries += int(segmentStats["table1_entries"].(int))
		}

		stats[fmt.Sprintf("segment_%d", i)] = segmentStats
		seg.mu.RUnlock()
	}

	stats["total_entries"] = totalEntries
	stats["rehashing_segments"] = rehashingSegments
	stats["total_segments"] = len(sd.segments)

	return stats
}
