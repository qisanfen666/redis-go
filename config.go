package main

import (
	"redis-go/evict"
	"redis-go/resp"
	"strconv"
	"strings"
)

// 最简配置表（key → 值）
var configMap = map[string]resp.RespValue{
	"save":             resp.Array{},
	"appendonly":       resp.Array{resp.BulkString("yes")},
	"appendfsync":      resp.Array{resp.BulkString("everysec")},
	"maxmemory":        resp.Array{resp.BulkString("0")},
	"maxmemory-policy": resp.Array{resp.BulkString("allkeys-lru")},
}

func configGet(arr resp.Array) resp.RespValue {
	if len(arr) < 3 || strings.ToUpper(string(arr[1].(resp.BulkString))) != "GET" {
		return resp.Error("ERR syntax error")
	}

	key := strings.ToLower(string(arr[2].(resp.BulkString)))

	if val, exist := configMap[key]; exist {
		return resp.Array{arr[2], val}
	}

	return resp.Array{}
}

func configSet(arr resp.Array) resp.RespValue {
	if len(arr) < 4 || strings.ToUpper(string(arr[1].(resp.BulkString))) != "SET" {
		return resp.Error("ERR syntax error")
	}

	key := strings.ToLower(string(arr[2].(resp.BulkString)))
	val := arr[3]
	switch key {
	case "maxmemory":
		bs, ok := val.(resp.BulkString)
		if !ok {
			return resp.Error("ERR value is not an integer or out of range")
		}
		maxBytes, err := strconv.ParseInt(string(bs), 10, 64)
		if err != nil || maxBytes < 0 {
			return resp.Error("ERR value is not an integer or out of range")
		}
		evict.Config.MaxMemory = maxBytes
		configMap[key] = resp.Array{resp.BulkString(strconv.FormatInt(maxBytes, 10))}
	case "maxmemory-policy":
		bs, ok := val.(resp.BulkString)
		if !ok {
			return resp.Error("ERR maxmemory-policy must be a string")
		}
		pol := strings.ToLower(string(bs))
		switch pol {
		case "noeviction", "allkeys-lru", "allkeys-lfu":
			evict.Config.Policy = evictPolicyFromString(pol)
			configMap[key] = resp.Array{resp.BulkString(pol)}
		default:
			return resp.Error("ERR unsupported maxmemory-policy")
		}
	case "appendonly":
		bs, ok := val.(resp.BulkString)
		if !ok {
			return resp.Error("ERR appendonly must be 'yes' or 'no'")
		}
		pol := strings.ToLower(string(bs))
		switch pol {
		case "yes":
			enableAOF()
			configMap[key] = resp.Array{resp.BulkString("yes")}
		case "no":
			disableAOF()
			configMap[key] = resp.Array{resp.BulkString("no")}
		default:
			return resp.Error("ERR appendonly must be 'yes' or 'no'")
		}
	case "appendfsync":
		bs, ok := val.(resp.BulkString)
		if !ok {
			return resp.Error("ERR appendfsync must be a string")
		}
		pol := strings.ToLower(string(bs))
		switch pol {
		case "always", "everysec", "no":
			setFsyncPolicy(pol)
			configMap[key] = resp.Array{resp.BulkString(pol)}
		default:
			return resp.Error("ERR unsupported appendfsync policy")
		}
	default:
		configMap[key] = resp.Array{val}
	}

	return resp.SimpleString("OK")
}

// isAppendOnlyEnabled 读取 configMap 中 appendonly 是否为 yes
func isAppendOnlyEnabled() bool {
	val, ok := configMap["appendonly"]
	if !ok {
		return false
	}
	arr, ok := val.(resp.Array)
	if !ok || len(arr) == 0 {
		return false
	}
	bs, ok := arr[0].(resp.BulkString)
	if !ok {
		return false
	}
	return strings.EqualFold(string(bs), "yes")
}

func configAppendFsync() string {
	val, ok := configMap["appendfsync"]
	if !ok {
		return "everysec"
	}
	arr, ok := val.(resp.Array)
	if !ok || len(arr) == 0 {
		return "everysec"
	}
	bs, ok := arr[0].(resp.BulkString)
	if !ok {
		return "everysec"
	}
	return strings.ToLower(string(bs))
}

func evictPolicyFromString(s string) evict.Policy {
	switch s {
	case "allkeys-lru":
		return evict.PolicyAllKeysLRU
	case "allkeys-lfu":
		return evict.PolicyAllKeysLFU
	default:
		return evict.PolicyNone // noeviction
	}
}
