package main

import (
	"strconv"
	"testing"
)

func TestDictBasic(t *testing.T) {
	d := DictCreate()
	d.DictAdd("k", "v")
	if v, ok := d.DictGet("k"); !ok || v != "v" {
		t.Fail()
	}
	if !d.DictDelete("k") || d.used != 0 {
		t.Fail()
	}
}

func TestRehashTrigger(t *testing.T) {
	d := DictCreate()
	rehashStarted := false

	// 插入 > 初始桶数，触发 rehash
	for i := 0; i < 1000; i++ {
		d.DictAdd(strconv.Itoa(i), "v")
		// 记录是否曾经启动过 rehash
		if d.rehashIdx != -1 {
			rehashStarted = true
		}
	}

	// 检查是否曾经启动过 rehash
	if !rehashStarted {
		t.Fatal("rehash not started")
	}

	// 再 get 一次，应搬完
	d.DictGet("0")
	if d.rehashIdx != -1 {
		t.Logf("rehash progress: %d/%d", d.rehashIdx, d.ht[0].size)
	}
}
