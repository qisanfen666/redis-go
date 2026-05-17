package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
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
	maxPipeline       = 256
	pipelineReadWait  = time.Millisecond // coalesce additional pipelined commands already in flight
)

func isNetTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

type Client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	mu   sync.RWMutex
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
	defer closeAOF()

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
	if err := initAOF(); err != nil {
		return err
	}
	if err := loadAOF(); err != nil {
		return err
	}
	if isAppendOnlyEnabled() {
		setFsyncPolicy(configAppendFsync())
		enableAOF()
	}
	//加载rdb
	return nil
}

func startServer() {
	ln, err := net.Listen("tcp", *redisAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
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
		conn.SetReadDeadline(time.Time{})
		cmd, err := readFullRESP(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			return
		}
		batch := [][]byte{cmd}

		for len(batch) < maxPipeline {
			if r.Buffered() == 0 {
				conn.SetReadDeadline(time.Now().Add(pipelineReadWait))
			}
			cmd2, err := readFullRESP(r)
			conn.SetReadDeadline(time.Time{})
			if err != nil {
				if isNetTimeout(err) {
					break
				}
				if errors.Is(err, io.EOF) {
					for _, c := range batch {
						writeCommandReply(w, c)
					}
					w.Flush()
					return
				}
				return
			}
			batch = append(batch, cmd2)
		}

		for _, c := range batch {
			writeCommandReply(w, c)
		}
		w.Flush()
	}
}

func writeCommandReply(w *bufio.Writer, cmd []byte) {
	val, _, err := resp.ParseRESP(cmd)
	if err != nil {
		w.Write(resp.Error("ERR protocol error").ToBytes())
		return
	}
	w.Write(HandleCommand(val).ToBytes())
}

func readFullRESP(r *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		buf = append(buf, line...)
		val, remaining, err := resp.ParseRESP(buf)
		if err != nil || len(remaining) > 0 {
			continue
		}
		if _, ok := val.(resp.Array); !ok {
			return nil, fmt.Errorf("ERR protocol error: expected array")
		}
		return buf, nil
	}
}
