package main

import (
	"redis-go/resp"
	"testing"
)

func TestPing(t *testing.T) {
	reply := HandleCommand(resp.Array{resp.BulkString("PING")})
	if s, ok := reply.(resp.SimpleString); !ok || s != "PONG" {
		t.Fatalf("unexpected reply: %v", reply)
	}
}

func TestSetGet(t *testing.T) {
	// 先清空 Store
	Store = make(map[string]string)

	HandleCommand(resp.Array{resp.BulkString("SET"), resp.BulkString("k"), resp.BulkString("v")})
	reply := HandleCommand(resp.Array{resp.BulkString("GET"), resp.BulkString("k")})
	if bs, ok := reply.(resp.BulkString); !ok || string(bs) != "v" {
		t.Fatalf("want v, got %v", reply)
	}
}

func TestDel(t *testing.T) {
	Store = make(map[string]string)
	Store["k"] = "v"
	reply := HandleCommand(resp.Array{resp.BulkString("DEL"), resp.BulkString("k")})
	if iv, ok := reply.(resp.Integer); !ok || iv != 1 {
		t.Fatalf("want 1, got %v", reply)
	}
	if _, exist := Store["k"]; exist {
		t.Fatal("key should be deleted")
	}
}
