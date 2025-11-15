package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"redis-go/resp"
	"redis-go/store"
	"sync"
	"time"
)

var Store = store.Store

var respArrayPool = sync.Pool{
	New: func() interface{} {
		return &resp.Array{}
	},
}
var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 16*1024)
	},
}
var bufioWriterPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, 16*1024)
	},
}
var maxPipeline = 256

func main() {

	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	// if err := initAOFIOUring("appendonly.aof"); err != nil {
	// 	log.Fatalf("init AOF failed: %v", err)
	// }
	// defer cleanup()

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

	r := bufioReaderPool.Get().(*bufio.Reader)
	r.Reset(conn)
	defer func() {
		bufioReaderPool.Put(r)
	}()
	w := bufioWriterPool.Get().(*bufio.Writer)
	w.Reset(conn)
	defer func() {
		bufioWriterPool.Put(w)
	}()

	//设置超时
	netConn, ok := conn.(interface {
		SetReadDeadline(t time.Time) error
	})
	hasDeadline := ok

	for {
		var responses []resp.RespValue

		//读取第一条命令
		raw, err := readFullRESP(r)
		if err != nil {
			return
		}

		//处理第一条命令
		val, _, err := resp.ParseRESP(append([]byte{}, raw...))
		if err != nil {
			w.Write(resp.Error("ERR invalid command").ToBytes())
			w.Flush()
			continue
		}
		responses = append(responses, HandleCommand(val))

		if hasDeadline {
			netConn.SetReadDeadline(time.Now().Add(10 * time.Microsecond))

			for i := 1; i < maxPipeline; i++ {
				raw, err := readFullRESP(r)
				if err != nil {
					break
				}
				val, _, err := resp.ParseRESP(append([]byte{}, raw...))
				if err != nil {
					w.Write(resp.Error("ERR invalid command").ToBytes())
					w.Flush()
					continue
				}
				responses = append(responses, HandleCommand(val))
			}

			netConn.SetReadDeadline(time.Time{})
		}

		for _, response := range responses {
			w.Write(response.ToBytes())
		}
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
			arr := respArrayPool.Get().(*resp.Array)
			arr.Reset()
			if err := arr.UnmarshalBinary(buf); err == nil {
				return buf, nil
			}
			respArrayPool.Put(arr)
		}
	}
}
