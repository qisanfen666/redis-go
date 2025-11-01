package main

import "redis-go/resp"

// 最简配置表（key → 值）
var configMap = map[string]resp.RespValue{
	"save":       resp.BulkString(""),   // 返回空字符串而不是空数组
	"appendonly": resp.BulkString("no"), // 明确返回 "no"
}

// ConfigGet 实现 CONFIG GET key [key ...]
func configGet(keys []resp.RespValue) resp.RespValue {
	arr := make(resp.Array, 0, len(keys)*2)
	for _, k := range keys {
		key := string(k.(resp.BulkString))
		if val, ok := configMap[key]; ok {
			arr = append(arr, resp.BulkString(key), val)
		} else {
			// 对于不存在的配置项，返回键和空字符串
			arr = append(arr, resp.BulkString(key), resp.BulkString(""))
		}
	}
	return arr
}
