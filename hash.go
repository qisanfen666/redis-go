package main

import "github.com/spaolacci/murmur3"

func murmur3Hash(key string) uint32 {
	h := murmur3.New32()
	h.Write([]byte(key))
	return h.Sum32()
}
