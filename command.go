package main

import (
	"redis-go/resp"
	"strings"
)

func HandleCommand(v resp.RespValue) resp.RespValue {
	arr, ok := v.(resp.Array)
	if !ok || len(arr) == 0 {
		return resp.Error("ERR unknow command")
	}
	cmd, ok := arr[0].(resp.BulkString)
	if !ok {
		return resp.Error("ERR unknow command")
	}
	switch strings.ToUpper(string(cmd)) {
	case "PING":
		return ping(arr)
	case "SET":
		return set(arr)
	case "GET":
		return get(arr)
	case "DEL":
		return del(arr)
	case "CONFIG":
		return config(arr)
	default:
		return resp.Error("ERR unknow command")
	}
}

func ping(arr resp.Array) resp.RespValue {
	if len(arr) == 1 {
		return resp.SimpleString("PONG")
	}
	return arr[1]
}

func set(arr resp.Array) resp.RespValue {
	if len(arr) != 3 {
		return resp.Error("ERR wrong command")
	}
	key := string(arr[1].(resp.BulkString))
	value := string(arr[2].(resp.BulkString))
	Store.DictAdd(key, value)
	return resp.SimpleString("OK")
}

func get(arr resp.Array) resp.RespValue {
	if len(arr) != 2 {
		return resp.Error("ERR wrong command")
	}
	key := string(arr[1].(resp.BulkString))
	val, ok := Store.DictGet(key)
	if !ok {
		return resp.Null{}
	}
	return resp.BulkString(val)
}

func del(arr resp.Array) resp.RespValue {
	deleted := 0
	for i := 1; i < len(arr); i++ {
		key := string(arr[i].(resp.BulkString))
		if _, exist := Store.DictGet(key); exist {
			Store.DictDelete(key)
			deleted++
		}
	}
	return resp.Integer(int64(deleted))
}

func config(arr resp.Array) resp.RespValue {
	if len(arr) < 3 || strings.ToUpper(string(arr[1].(resp.BulkString))) != "GET" {
		return resp.Error("ERR wrong number of arguments for 'config|get' command")
	}

	// 获取所有键
	keys := arr[2:]

	// 检查是否有通配符 "*"
	if len(keys) == 1 {
		pattern := string(keys[0].(resp.BulkString))
		if pattern == "*" {
			// 返回所有配置项
			allKeys := make([]resp.RespValue, 0, len(configMap)*2)
			for k, v := range configMap {
				allKeys = append(allKeys, resp.BulkString(k), v)
			}
			return resp.Array(allKeys)
		}
	}

	return configGet(keys)
}
