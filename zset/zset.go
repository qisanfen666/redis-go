package zset

import (
	"math/rand"
	"time"
)

const maxLevel = 32
const p = 0.25

type zslNode struct {
	key     string
	score   float64
	forward []*zslNode
}

type SkipList struct {
	header *zslNode
	level  int
	length int64
}

type Zset struct {
	dict map[string]float64
	zsl  *SkipList
}

func NewSkipList() *SkipList {
	source := rand.NewSource(time.Now().UnixNano())
	rand.New(source)

	header := &zslNode{
		forward: make([]*zslNode, maxLevel),
	}
	return &SkipList{
		header: header,
		level:  1,
		length: 0,
	}
}

func NewZset() *Zset {
	return &Zset{
		dict: make(map[string]float64),
		zsl:  NewSkipList(),
	}
}

func (s *SkipList) randomLevel() int {
	level := 1
	for rand.Float64() < p && level < maxLevel {
		level++
	}
	return level
}

func (z *Zset) Add(key string, score float64) {
	oldScore, exist := z.dict[key]
	if exist {
		if oldScore == score {
			return
		}

		delete(z.dict, key)
		z.zsl.deleteNode(key, oldScore)
	}

	z.dict[key] = score

	z.zsl.insertNode(key, score)
}

func (z *Zset) Score(key string) (float64, bool) {
	score, exist := z.dict[key]
	return score, exist
}

func (z *Zset) Range(start, end int64) []string {
	return z.zsl.Range(start, end)
}

func (z *Zset) Remove(key string, score float64) bool {
	_, exist := z.dict[key]

	if !exist {
		return false
	}

	delete(z.dict, key)

	z.zsl.deleteNode(key, score)

	return true
}
func (s *SkipList) insertNode(key string, score float64) {
	update := make([]*zslNode, maxLevel)
	x := s.header

	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil &&
			(x.forward[i].score < score ||
				(x.forward[i].score == score && x.forward[i].key < key)) {
			x = x.forward[i]
		}
		update[i] = x
	}

	if x.forward[0] != nil && x.forward[0].key == key {
		x.forward[0].score = score
		return
	}

	level := s.randomLevel()
	if level > s.level {
		for i := s.level; i < level; i++ {
			update[i] = s.header
		}
		s.level = level
	}

	n := &zslNode{
		key:     key,
		score:   score,
		forward: make([]*zslNode, level),
	}
	for i := 0; i < level; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}
	s.length++
}

func (s *SkipList) deleteNode(key string, score float64) {
	update := make([]*zslNode, maxLevel)
	x := s.header

	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil &&
			(x.forward[i].score < score ||
				(x.forward[i].score == score && x.forward[i].key < key)) {
			x = x.forward[i]
		}
		update[i] = x
	}

	x = x.forward[0]
	if x == nil || x.key != key {
		return
	}

	for i := 0; i < s.level; i++ {
		if update[i].forward[i] != x {
			break
		}
		update[i].forward[i] = x.forward[i]
	}

	for s.level > 1 && s.header.forward[s.level-1] == nil {
		s.level--
	}
	s.length--
}

func (s *SkipList) Range(start, end int64) []string {
	if start < 0 {
		start = 0
	}
	if end >= s.length {
		end = s.length - 1
	}
	if start > end {
		return nil
	}

	x := s.header
	for i := int64(0); i <= start; i++ {
		if x.forward[0] == nil {
			break
		}
		x = x.forward[0]
	}

	var out []string
	for i := start; i <= end; i++ {
		if x == nil {
			break
		}
		out = append(out, x.key)
		x = x.forward[0]
	}
	return out
}

func (z *Zset) Len() int64 {
	return z.zsl.length
}
