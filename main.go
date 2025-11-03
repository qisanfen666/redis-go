package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"redis-go/resp"
)

var Store = NewSegDict()

func main() {

	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	err := loadAOF("appendonly.aof")
	if err != nil {
		log.Fatalf("loadAOF:%v", err)
	}
	_ = openAOF("appendonly.aof")
	startAOFSync()

	lis, err := net.Listen("tcp", ":6380")

	if err != nil {
		panic(err)
	}

	fmt.Println("redis-go start")

	for {
		conn, err := lis.Accept()
		if err != nil {
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		raw, err := readFullRESP(r)
		if err != nil {
			return
		}
		val, rest, err := resp.ParseRESP(append([]byte{}, raw...))
		if err != nil {
			continue
		}
		_ = rest

		reply := HandleCommand(val)

		w.Write(reply.ToBytes())
		w.Flush()
	}
}

func readFullRESP(r *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		buf = append(buf, line...)
		_, remaining, err := resp.ParseRESP(buf)
		if err == nil && len(remaining) == 0 {
			return buf, nil
		}
	}
}
