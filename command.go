package main

import (
	"redis-go/resp"
	"redis-go/ttl"
	"strconv"
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
	case "EXPIRE":
		return expire(arr)
	case "TTL":
		return handleTTL(arr)
	case "CONFIG":
		return config(arr)
	case "BGREWRITEAOF":
		//bgReWriteAOF()
		return resp.SimpleString("Background AOF rewrite started")
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
	//appendAOF([]string{"SET", key, value})
	Store.Add(key, value)
	return resp.SimpleString("OK")
}

func get(arr resp.Array) resp.RespValue {
	if len(arr) != 2 {
		return resp.Error("ERR wrong command")
	}
	key := string(arr[1].(resp.BulkString))
	val, ok := Store.Get(key)
	if !ok {
		return resp.Null{}
	}
	return resp.BulkString(val)
}

func del(arr resp.Array) resp.RespValue {
	deleted := 0
	for i := 1; i < len(arr); i++ {
		key := string(arr[i].(resp.BulkString))
		if _, exist := Store.Get(key); exist {
			Store.Delete(key)
			//appendAOF([]string{"DEL", key})
			deleted++
		}
	}
	return resp.Integer(int64(deleted))
}

func expire(arr resp.Array) resp.RespValue {
	if len(arr) != 3 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))
	if _, exist := Store.Get(key); !exist {
		return resp.Error("ERR no such key")
	}
	ttlstr := string(arr[2].(resp.BulkString))
	sec, _ := strconv.Atoi(ttlstr)
	ttl.SetTTL(key, sec)
	return resp.Integer(1)
}

func handleTTL(arr resp.Array) resp.RespValue {
	if len(arr) != 2 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))
	left := ttl.TTL(key)
	return resp.Integer(left)
}

func config(arr resp.Array) resp.RespValue {
	switch strings.ToUpper(string(arr[1].(resp.BulkString))) {
	case "GET":
		return configGet(arr)
	case "SET":
		return configSet(arr)
	default:
		return resp.Error("ERR syntax error")
	}
}
