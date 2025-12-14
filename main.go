package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"redis-go/raft"
	"redis-go/resp"
	"redis-go/store"
	"strings"
	"sync"
	"time"
)

var Store = store.Store

type clientConn struct {
	conn net.Conn
	tx   *ClientTx
	r    *bufio.Reader
	w    *bufio.Writer
	mu   sync.RWMutex
}

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

//var addrs = []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}

var raftNode *raft.RaftNode

var (
	redisAddr = flag.String("redis", ":6380", "redis listen addr")
	pprofAddr = flag.String("pprof", ":0", "pprof addr,:0=disable")
)

func main() {

	//raft
	id := flag.Int("id", 1, "node ID")
	addr := flag.String("addr", ":7001", "listen addr")
	peers := flag.String("peers", "", "comma separated list of peers")
	flag.Parse()
	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}
	raftNode = raft.NewRaftNode(*id, *addr, peerList)

	if *pprofAddr != ":0" {
		go func() {
			log.Println(http.ListenAndServe(*pprofAddr, nil))
		}()
	}

	//加载AOF
	if err := initAOF(); err != nil {
		log.Fatalf("load AOF failed: %v", err)
	}
	defer closeAOF()

	go aofFsyncEverySec()

	if err := loadAOF(); err != nil {
		log.Fatalf("load AOF failed: %v", err)
	}

	//加载RDB
	if err := loadRDB(); err != nil {
		log.Fatalf("load RDB failed: %v", err)
	}
	if len(os.Args) > 1 && os.Args[1] == "bgsave" {
		if err := doSave(); err != nil {
			log.Fatalf("BGSAVE failed: %v", err)
		}
		log.Println("BGSAVE completed")
		return
	}

	lis, err := net.Listen("tcp", *redisAddr)

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

	cc := &clientConn{
		conn: conn,
		tx:   NewClientTx(),
		r:    r,
		w:    w,
	}

	defer cc.Close()

	//设置超时
	netConn, ok := cc.conn.(interface {
		SetReadDeadline(t time.Time) error
	})
	hasDeadline := ok

	for {
		var responses []resp.RespValue

		//读取第一条命令
		raw, err := readFullRESP(cc.r)
		if err != nil {
			return
		}

		//处理第一条命令
		val, _, err := resp.ParseRESP(append([]byte{}, raw...))
		if err != nil {
			cc.w.Write(resp.Error("ERR invalid command").ToBytes())
			cc.w.Flush()
			continue
		}
		responses = append(responses, cc.handleOne(val))

		if hasDeadline {
			netConn.SetReadDeadline(time.Now().Add(10 * time.Microsecond))

			for i := 1; i < maxPipeline; i++ {
				raw, err := readFullRESP(cc.r)
				if err != nil {
					break
				}
				val, _, err := resp.ParseRESP(append([]byte{}, raw...))
				if err != nil {
					cc.w.Write(resp.Error("ERR invalid command").ToBytes())
					cc.w.Flush()
					continue
				}
				responses = append(responses, cc.handleOne(val))
			}

			netConn.SetReadDeadline(time.Time{})
		}

		for _, response := range responses {
			cc.w.Write(response.ToBytes())
		}
		cc.w.Flush()
	}
}

func (cc *clientConn) handleOne(val resp.RespValue) resp.RespValue {
	arr, ok := val.(resp.Array)
	if !ok || len(arr) == 0 {
		return resp.Error("ERR unknow command")
	}
	cmd, ok := arr[0].(resp.BulkString)
	if !ok {
		return resp.Error("ERR unknow command")
	}

	switch strings.ToUpper(string(cmd)) {
	case "MULTI", "EXEC", "DISCARD":
		return HandleCommand(val, cc)
	}
	return cc.tx.enqueue(func() resp.RespValue {
		return HandleCommand(val, cc)
	})
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

func (cc *clientConn) Close() {
	psHub.removeAllSubscriptions(cc)
	cc.conn.Close()
}
