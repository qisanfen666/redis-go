package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"redis-go/resp"
	"redis-go/store"
	"sync"
	"time"
)

var (
	Store       = store.Store
	redisAddr   = flag.String("addr", ":6380", "redis server address")
	pprofAddr   = flag.String("pprof", ":6060", "pprof address")
	maxPipeline = 256
)

type Client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	mu   sync.RWMutex
}

var respArrayPool = sync.Pool{
	New: func() interface{} {
		return &resp.Array{}
	},
}
var readerPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 16*1024)
	},
}
var writerPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, 16*1024)
	},
}

func main() {
	flag.Parse()

	//性能分析server
	if *pprofAddr != "" {
		go func() {
			log.Printf("pprof server started on %s", *pprofAddr)
			http.ListenAndServe(*pprofAddr, nil)
		}()
	}

	//加载持久化
	if err := loadPersistence(); err != nil {
		log.Printf("WARN: load persistence failed: %v", err)
	}

	//启动服务
	startServer()

	//加载AOF
	// if err := initAOF(); err != nil {
	// 	log.Fatalf("load AOF failed: %v", err)
	// }

	// defer closeAOF()

	// enableAOF()

	// if err := loadAOF(); err != nil {
	// 	log.Fatalf("load AOF failed: %v", err)
	// }

	// //加载RDB
	// if err := loadRDB(); err != nil {
	// 	log.Fatalf("load RDB failed: %v", err)
	// }
	// if len(os.Args) > 1 && os.Args[1] == "bgsave" {
	// 	if err := doSave(); err != nil {
	// 		log.Fatalf("BGSAVE failed: %v", err)
	// 	}
	// 	log.Println("BGSAVE completed")
	// 	return
	// }

	//启动服务器实例
	// lis, err := net.Listen("tcp", *redisAddr)

	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println("redis-go start")

	// for {
	// 	conn, err := lis.Accept()
	// 	if err != nil {
	// 		continue
	// 	}
	// 	go handleConn(conn)
	// }
}

func loadPersistence() error {
	//加载aof
	//加载rdb
	return nil
}

func startServer() {
	ln, err := net.Listen("tcp", *redisAddr)
	if err != nil {
		log.Printf("listen failed: %v", err)
	}
	defer ln.Close()

	log.Printf("redis server started on %s", *redisAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept failed: %v", err)
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	//从pool里获取reader/writer
	r := readerPool.Get().(*bufio.Reader)
	r.Reset(conn)
	defer func() {
		readerPool.Put(r)
	}()

	w := writerPool.Get().(*bufio.Writer)
	w.Reset(conn)
	defer func() {
		w.Flush()
		writerPool.Put(w)
	}()

	//设置连接属性
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	for {
		var batch [][]byte
		conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))

		for i := 0; i < maxPipeline; i++ {
			cmd, err := readFullRESP(r)
			if err != nil {
				return
			}
			batch = append(batch, cmd)
		}
		conn.SetReadDeadline(time.Time{})

		for _, cmd := range batch {
			val, _, err := resp.ParseRESP(cmd)
			if err != nil {
				errResp := resp.Error("ERR protocol error")
				w.Write(errResp.ToBytes())
				continue
			}
			resp := HandleCommand(val)
			w.Write(resp.ToBytes())
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
