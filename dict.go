package main

import (
	"hash/fnv"
)

// 主体字典
type Dict struct {
	ht        [2]*hashTable
	rehashIdx int
	used      int64
}

type hashTable struct {
	size  int
	mask  int
	table []*dictEntry
}

type dictEntry struct {
	key  string
	val  string
	next *dictEntry
}

// 哈希函数 FNV-1a
func fnv1aHash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func DictCreate() *Dict {
	d := &Dict{
		rehashIdx: -1, //-1表示没开始rehash
		used:      0,
	}

	d.ht[0] = &hashTable{
		size:  4,
		mask:  3,
		table: make([]*dictEntry, 4),
	}

	d.ht[1] = nil

	return d
}

func (d *Dict) DictAdd(key, val string) {
	//若在rehash,先走一步
	if d.rehashIdx != -1 {
		d.DictRehash(1)
	}

	hash := fnv1aHash(key)
	idx := hash & uint32(d.ht[0].mask)

	entry := d.ht[0].table[idx]
	for entry != nil {
		if entry.key == key {
			entry.val = val
			return
		}
		entry = entry.next
	}

	newEntry := &dictEntry{
		key:  key,
		val:  val,
		next: d.ht[0].table[idx],
	}
	d.ht[0].table[idx] = newEntry
	d.used++

	//检查负载因子
	if float64(d.used)/float64(d.ht[0].size) >= 1 && d.ht[1] == nil {
		newSize := d.ht[0].size * 2

		d.ht[1] = &hashTable{
			size:  newSize,
			mask:  newSize - 1,
			table: make([]*dictEntry, newSize),
		}

		d.rehashIdx = 0
	}
}

func (d *Dict) DictGet(key string) (string, bool) {
	//若在rehash,先走一步
	if d.rehashIdx != -1 {
		d.DictRehash(1)
	}

	hash := fnv1aHash(key)

	//先查ht[0]
	idx0 := hash & uint32(d.ht[0].mask)
	entry := d.ht[0].table[idx0]
	for entry != nil {
		if entry.key == key {
			return entry.val, true
		}
		entry = entry.next
	}

	//查ht[1]
	if d.ht[1] != nil {
		idx1 := hash & uint32(d.ht[1].mask)
		entry := d.ht[1].table[idx1]
		for entry != nil {
			if entry.key == key {
				return entry.val, true
			}
			entry = entry.next
		}
	}

	return "", false
}

func (d *Dict) DictDelete(key string) bool {
	//若在rehash,先走一步
	if d.rehashIdx != -1 {
		d.DictRehash(1)
	}

	hash := fnv1aHash(key)

	//删除ht[0]中的节点
	idx0 := hash & uint32(d.ht[0].mask)
	entry := d.ht[0].table[idx0]
	var prev *dictEntry
	for entry != nil {
		if entry.key == key {
			if prev == nil {
				d.ht[0].table[idx0] = entry.next
			} else {
				prev.next = entry.next
			}
			d.used--
			return true
		}
		prev = entry
		entry = entry.next
	}

	//删除ht[1]中的节点
	if d.ht[1] != nil {
		idx1 := hash & uint32(d.ht[1].mask)
		entry := d.ht[1].table[idx1]
		var prev *dictEntry
		for entry != nil {
			if entry.key == key {
				if prev == nil {
					d.ht[1].table[idx1] = entry.next
				} else {
					prev.next = entry.next
				}
				d.used--
				return true
			}
			prev = entry
			entry = entry.next
		}
	}

	return false
}

// 渐进式rehash
func (d *Dict) DictRehash(steps int) int {
	if d.rehashIdx == -1 {
		return 0
	}

	moved := 0
	for moved < steps && d.rehashIdx < d.ht[0].size {
		//处理第rehashIdx个桶
		entry := d.ht[0].table[d.rehashIdx]
		for entry != nil {
			next := entry.next

			hash := fnv1aHash(entry.key)
			newIdx := hash & uint32(d.ht[1].mask)

			//插入ht[1]头部
			entry.next = d.ht[1].table[newIdx]
			d.ht[1].table[newIdx] = entry

			entry = next
			moved++
		}

		d.ht[0].table[d.rehashIdx] = nil
		d.rehashIdx++
	}

	if d.rehashIdx >= d.ht[0].size {
		d.ht[0] = d.ht[1]
		d.ht[1] = nil
		d.rehashIdx = -1
		d.ht[0].size = len(d.ht[0].table)
		d.ht[0].mask = d.ht[0].size - 1
	}

	return moved
}
