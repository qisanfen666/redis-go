package hash

import "github.com/spaolacci/murmur3"

const murmur3Seed = 0x12345678

func Murmur3Hash(key string) uint32 {
	h := murmur3.New32WithSeed(murmur3Seed)
	h.Write([]byte(key))
	return h.Sum32()
}
