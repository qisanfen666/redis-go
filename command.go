package main

import (
	"fmt"
	"redis-go/evict"
	"redis-go/list"
	"redis-go/resp"
	"redis-go/ttl"
	"redis-go/zset"
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
	case "ZADD":
		return zadd(arr)
	case "ZRANGE":
		return zrange(arr)
	case "ZSCORE":
		return zscore(arr)
	case "ZREM":
		return zrem(arr)
	case "LPUSH":
		return lpush(arr)
	case "RPUSH":
		return rpush(arr)
	case "LPOP":
		return lpop(arr)
	case "RPOP":
		return rpop(arr)
	case "LRANGE":
		return lrange(arr)
	// case "MULTI":
	// 	return cc.tx.multi()
	// case "DISCARD":
	// 	return cc.tx.discard()
	// case "EXEC":
	// 	return cc.tx.exec()
	// case "PUBLISH":
	// 	return publish(arr)
	// case "SUBSCRIBE":
	// 	return subscribe(arr, cc)
	// case "UNSUBSCRIBE":
	// 	return unsubscribe(arr, cc)
	case "CONFIG":
		return config(arr)
	case "BGREWRITEAOF":
		bgReWriteAOF()
		return resp.SimpleString("Background AOF rewrite started")
	case "BGSAVE":
		return bgsave()
	case "INFO":
		return info(arr)
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
	if len(arr) < 3 {
		return resp.Error("ERR wrong command")
	}
	key := string(arr[1].(resp.BulkString))
	value := string(arr[2].(resp.BulkString))

	var expireTime int = -1

	for i := 3; i < len(arr); i++ {
		option := strings.ToUpper(string(arr[i].(resp.BulkString)))
		switch option {
		case "EX":
			if i+1 >= len(arr) {
				return resp.Error("ERR wrong number of arguments")
			}
			sec, _ := strconv.Atoi(string(arr[i+1].(resp.BulkString)))
			expireTime = int(sec)
			i++
		default:
			return resp.Error("ERR unknown option")
		}
	}
	appendAOF([]string{"SET", key, value})
	Store.Set(key, value)

	evict.UpdateMemoryUsage(evict.EstimateMemoryUsage(key, value))
	evict.Touch(key)
	maybeEvict()

	if expireTime > 0 {
		ttl.SetTTL(key, expireTime)
	} else {
		ttl.DelTTL(key)
	}

	return resp.SimpleString("OK")
}

func get(arr resp.Array) resp.RespValue {
	if len(arr) != 2 {
		return resp.Error("ERR wrong command")
	}
	key := string(arr[1].(resp.BulkString))
	if ttl.IsExpired(key) {
		return resp.Null{}
	}
	val, ok := Store.Get(key)
	if !ok {
		return resp.Null{}
	}
	evict.Touch(key)
	return resp.BulkString(val)
}

func del(arr resp.Array) resp.RespValue {
	deleted := 0
	for i := 1; i < len(arr); i++ {
		key := string(arr[i].(resp.BulkString))
		if _, exist := Store.Get(key); exist {
			Store.Delete(key)
			appendAOF([]string{"DEL", key})
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

func zadd(arr resp.Array) resp.RespValue {
	if len(arr)%2 != 0 || len(arr) < 4 {
		return resp.Error("ERR syntax error")
	}

	key := string(arr[1].(resp.BulkString))
	zs, ok := Store.Zsets[key]
	if !ok {
		zs = zset.NewZset()
		Store.Zsets[key] = zs
	}
	added := 0
	for i := 2; i < len(arr); i += 2 {
		score, err := strconv.ParseFloat(string(arr[i].(resp.BulkString)), 64)
		if err != nil {
			return resp.Error("ERR syntax error")
		}
		member := string(arr[i+1].(resp.BulkString))
		_, exist := zs.Score(member)
		zs.Add(member, score)
		if !exist {
			added++
		}
	}

	return resp.Integer(int64(added))
}

func zrange(arr resp.Array) resp.RespValue {
	if len(arr) < 4 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))
	start, _ := strconv.Atoi(string(arr[2].(resp.BulkString)))
	end, _ := strconv.Atoi(string(arr[3].(resp.BulkString)))

	//withScores := len(arr) == 5 && strings.ToUpper(string(arr[4].(resp.BulkString))) == "WITHSCORES"

	zs, ok := Store.Zsets[key]
	if !ok {
		return resp.Array{}
	}
	nodes := zs.Range(int64(start), int64(end))
	capacity := len(nodes)
	out := make(resp.Array, 0, capacity)
	for _, node := range nodes {
		out = append(out, resp.BulkString(node))
	}

	return out
}

func zscore(arr resp.Array) resp.RespValue {
	if len(arr) < 3 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))
	member := string(arr[2].(resp.BulkString))
	score, ok := Store.Zsets[key].Score(member)
	if !ok {
		return resp.Null{}
	}

	return resp.BulkString(strconv.FormatFloat(score, 'f', -1, 64))
}

func zrem(arr resp.Array) resp.RespValue {
	if len(arr) < 3 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))
	removed := 0
	for i := 2; i < len(arr); i++ {
		member := string(arr[i].(resp.BulkString))
		score, _ := Store.Zsets[key].Score(member)
		ok := Store.Zsets[key].Remove(member, score)
		if ok {
			removed++
		}
	}
	if Store.Zsets[key].Len() == 0 {
		delete(Store.Zsets, key)
	}

	return resp.Integer(int64(removed))
}

func lpush(arr resp.Array) resp.RespValue {
	if len(arr) < 3 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))

	l, exist := Store.Lists[key]
	if !exist {
		l = list.NewList()
		Store.Lists[key] = l
	}
	for i := 2; i < len(arr); i++ {
		val := string(arr[i].(resp.BulkString))
		l.LPush(val)
	}
	maybeEvict()
	return resp.Integer(int64(l.Len()))
}

func rpush(arr resp.Array) resp.RespValue {
	if len(arr) < 3 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))

	l, exist := Store.Lists[key]
	if !exist {
		l = list.NewList()
		Store.Lists[key] = l
	}
	for i := 2; i < len(arr); i++ {
		val := string(arr[i].(resp.BulkString))
		l.RPush(val)
	}
	maybeEvict()
	return resp.Integer(int64(l.Len()))
}

func lpop(arr resp.Array) resp.RespValue {
	if len(arr) < 2 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))
	var count int
	if len(arr) == 3 {
		count, _ = strconv.Atoi(string(arr[2].(resp.BulkString)))
	} else {
		count = 1
	}
	l, exist := Store.Lists[key]
	if !exist {
		return resp.Null{}
	}
	out := make(resp.Array, 0, count)
	var val string
	for i := 0; i < count; i++ {
		val = l.LPop()
		out = append(out, resp.BulkString(val))
		if l.Len() == 0 {
			delete(Store.Lists, key)
			return out
		}
	}

	return out
}

func rpop(arr resp.Array) resp.RespValue {
	if len(arr) < 2 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))
	var count int
	if len(arr) == 3 {
		count, _ = strconv.Atoi(string(arr[2].(resp.BulkString)))
	} else {
		count = 1
	}
	l, exist := Store.Lists[key]
	if !exist {
		return resp.Null{}
	}
	out := make(resp.Array, 0, count)
	var val string
	for i := 0; i < count; i++ {
		val = l.RPop()
		out = append(out, resp.BulkString(val))
		if l.Len() == 0 {
			delete(Store.Lists, key)
			return out
		}
	}

	return out
}

func lrange(arr resp.Array) resp.RespValue {
	if len(arr) != 4 {
		return resp.Error("ERR syntax error")
	}
	key := string(arr[1].(resp.BulkString))
	start, _ := strconv.Atoi(string(arr[2].(resp.BulkString)))
	end, _ := strconv.Atoi(string(arr[3].(resp.BulkString)))

	l, exist := Store.Lists[key]
	if !exist {
		return resp.Array{}
	}
	vals := l.LRange(start, end)
	out := make(resp.Array, 0, len(vals))
	for _, val := range vals {
		out = append(out, resp.BulkString(val))
	}

	return out
}

// func publish(arr resp.Array) resp.RespValue {
// 	if len(arr) != 3 {
// 		return resp.Error("ERR syntax error")
// 	}

// 	channel := string(arr[1].(resp.BulkString))
// 	message := string(arr[2].(resp.BulkString))
// 	n := psHub.publish(channel, message)

// 	return resp.Integer(int64(n))
// }

// func subscribe(arr resp.Array, cc *clientConn) resp.RespValue {
// 	if len(arr) < 2 {
// 		return resp.Error("ERR syntax error")
// 	}

// 	var channel string
// 	out := resp.Array{}

// 	for i := 1; i < len(arr); i++ {
// 		channel = string(arr[i].(resp.BulkString))
// 		count := psHub.subscribe(channel, cc)
// 		out = append(out,
// 			resp.BulkString("subscribe"),
// 			resp.BulkString(channel),
// 			resp.Integer(count),
// 		)
// 	}

// 	return out
// }

// func unsubscribe(arr resp.Array, cc *clientConn) resp.RespValue {
// 	if len(arr) < 2 {
// 		return resp.Error("ERR syntax error")
// 	}

// 	var channel string
// 	out := resp.Array{}

// 	for i := 1; i < len(arr); i++ {
// 		channel = string(arr[i].(resp.BulkString))
// 		count := psHub.unsubscribe(channel, cc)
// 		out = append(out,
// 			resp.BulkString("unsubscribe"),
// 			resp.BulkString(channel),
// 			resp.Integer(count),
// 		)
// 	}

// 	return out
// }

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

func info(arr resp.Array) resp.RespValue {
	var b strings.Builder
	b.WriteString("# Server\r\n")
	b.WriteString("redis_version:0.1.0\r\n")
	b.WriteString("# Keyspace\r\n")
	b.WriteString(fmt.Sprintf("expired_keys:%d\r\n", ttl.ExpiredKeys()))
	return resp.BulkString(b.String())
}

func maybeEvict() {
	if evict.Config.MaxMemory <= 0 {
		return
	}
	for evict.GetMemoryUsage() > evict.Config.MaxMemory {
		evict.Evict(1) // 一次删一个，平滑
	}
}
