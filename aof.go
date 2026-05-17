package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"redis-go/resp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const aofFileName = "appendonly.aof"

type aof struct {
	mu sync.Mutex
	enabled bool
	file *os.File
	buf *bufio.Writer
	policy string
}

var (
	globalAOF    aof
	fsyncCancel  context.CancelFunc
	aofReplaying atomic.Bool
)

func initAOF() error {
	globalAOF.mu.Lock()
	defer globalAOF.mu.Unlock()

	if globalAOF.file != nil {
		return nil
	}

	f, err := os.OpenFile(aofFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
 	if err != nil {
 		return err
 	}

	globalAOF.file = f
	globalAOF.buf = bufio.NewWriter(f)
	if globalAOF.policy == "" {
		globalAOF.policy = "everysec"
	}

	return nil
}

func closeAOF() {
	globalAOF.mu.Lock()
	defer globalAOF.mu.Unlock()

	stopFsyncWorkerLocked()

 	if globalAOF.buf != nil {
 		_ = globalAOF.buf.Flush()
 	}
 	if globalAOF.file != nil {
 		_ = globalAOF.file.Sync()
 		_ = globalAOF.file.Close()
 	}
	globalAOF.file = nil
	globalAOF.buf = nil
	globalAOF.enabled = false
	log.Println("AOF disabled")
}

func enableAOF() {
	globalAOF.mu.Lock()
	defer globalAOF.mu.Unlock()

	if globalAOF.file == nil {
		f,err := os.OpenFile(aofFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Println("enableAOF open file error:", err)
			return
		}
		globalAOF.file = f
		globalAOF.buf = bufio.NewWriter(f)
	}

	globalAOF.enabled = true
	startFsyncWorkerLocked()
	log.Println("AOF enabled")
}

func disableAOF() {
	globalAOF.mu.Lock()
	defer globalAOF.mu.Unlock()

	if !globalAOF.enabled {
		return
	}

	stopFsyncWorkerLocked()
	if globalAOF.buf != nil {
		_ = globalAOF.buf.Flush()
	}
	if globalAOF.file != nil {
		_ = globalAOF.file.Sync()
		_ = globalAOF.file.Close()
	}
	globalAOF.file = nil
	globalAOF.buf = nil
	globalAOF.enabled = false
	log.Println("AOF disabled")
}

func setFsyncPolicy(pol string) {
	globalAOF.mu.Lock()
	defer globalAOF.mu.Unlock()

	globalAOF.policy = pol
	if globalAOF.enabled {
		stopFsyncWorkerLocked()
		startFsyncWorkerLocked()
	}
}

func appendAOF(args []string) {
	if aofReplaying.Load() {
		return 
	}

	globalAOF.mu.Lock()
	defer globalAOF.mu.Unlock()

	if !globalAOF.enabled || globalAOF.buf == nil {
		return 
	}

	writeRespArray(globalAOF.buf,args)

	switch globalAOF.policy {
		case "always":
			_ = globalAOF.buf.Flush()
			_ = globalAOF.file.Sync()
		case "everysec":
			_ = globalAOF.buf.Flush()
		case "no":
		default:
			log.Println("unknown fsync policy:", globalAOF.policy)
	}
}

func loadAOF() error {
	f,err := os.Open(aofFileName)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	aofReplaying.Store(true)
	defer aofReplaying.Store(false)

	r := bufio.NewReader(f)
	for {
		raw, err := readFullRESP(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[AOF] readFullRESP error: %v", err)
			continue
		}

		val, _, err := resp.ParseRESP(raw)
		if err != nil {
			log.Printf("[AOF] ParseRESP error: %v", err)
			continue
		}
		HandleCommand(val)
	}
	return nil
}

func writeRespArray(w *bufio.Writer,args []string) {
	_, _ = w.WriteString("*")
	_, _ = w.WriteString(strconv.Itoa(len(args)))
	_, _ = w.WriteString("\r\n")
	for _, arg := range args {
		_, _ = w.WriteString("$")
		_, _ = w.WriteString(strconv.Itoa(len(arg)))
		_, _ = w.WriteString("\r\n")
		_, _ = w.WriteString(arg)
		_, _ = w.WriteString("\r\n")
	}
}

func startFsyncWorkerLocked() {
	if globalAOF.policy != "everysec" {
		return 
	}
	stopFsyncWorkerLocked()
	
	ctx,cancel := context.WithCancel(context.Background())
	fsyncCancel = cancel
	go aofFsyncEverySec(ctx)
}

func stopFsyncWorkerLocked() {
	if fsyncCancel != nil {
		fsyncCancel()
		fsyncCancel = nil
	}
}

func aofFsyncEverySec(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return 
		case <-tick.C:
			globalAOF.mu.Lock()
			if globalAOF.enabled && globalAOF.buf != nil {
				_ = globalAOF.buf.Flush()
			}
			if globalAOF.enabled && globalAOF.file != nil {
				_ = globalAOF.file.Sync()
			}
			globalAOF.mu.Unlock()
		}
	}
}

func bgReWriteAOF() {
	log.Println("bgRewriteAOF start")

	globalAOF.mu.Lock()
	defer globalAOF.mu.Unlock()

	tmp := aofFileName + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Printf("bgrewriteaof create: %v", err)
		return 
	}
	w := bufio.NewWriter(f)

	Store.Scan(func(key,value string) {
		writeRespArray(w,[]string{"SET",key,value})
	})

	_ = w.Flush()
	_ = f.Sync()
	_ = f.Close()

	// Windows 上必须先关闭正在追加的 AOF，否则 Remove/Rename 会失败，.tmp 会残留
	if globalAOF.buf != nil {
		_ = globalAOF.buf.Flush()
		globalAOF.buf = nil
	}
	if globalAOF.file != nil {
		_ = globalAOF.file.Close()
		globalAOF.file = nil
	}

	if err := os.Remove(aofFileName); err != nil && !os.IsNotExist(err) {
		log.Printf("bgrewriteaof remove old: %v", err)
		return
	}
	if err := os.Rename(tmp, aofFileName); err != nil {
		log.Printf("bgrewriteaof rename: %v", err)
		return
	}

	nf, err := os.OpenFile(aofFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("bgrewriteaof reopen: %v", err)
		return
	}
	globalAOF.file = nf
	globalAOF.buf = bufio.NewWriter(nf)

	log.Println("bgrewriteaof done")
}