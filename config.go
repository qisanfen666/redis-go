package main

import (
	"redis-go/resp"
	"strings"
)

// 最简配置表（key → 值）
var configMap = map[string]resp.RespValue{
	"save":       resp.Array{},
	"appendonly": resp.Array{resp.BulkString("no")},
}

func configGet(arr resp.Array) resp.RespValue {
	if len(arr) < 3 || strings.ToUpper(string(arr[1].(resp.BulkString))) != "GET" {
		return resp.Error("ERR syntax error")
	}

	key := arr[2].(resp.BulkString)

	if val, exist := configMap[string(key)]; exist {
		result := make(resp.Array, 2)
		result[0] = key
		result[1] = val
		return result
	}

	return resp.Null{}
}

func configSet(arr resp.Array) resp.RespValue {
	if len(arr) < 4 || strings.ToUpper(string(arr[1].(resp.BulkString))) != "SET" {
		return resp.Error("ERR syntax error")
	}

	key := arr[2].(resp.BulkString)
	val := arr[3]
	configMap[string(key)] = resp.Array{val}

	if key == "appendonly" && string(val.(resp.BulkString)) == "yes" {
		//_ = openAOF("appendonly.aof")
	}
	return resp.SimpleString("OK")
}
