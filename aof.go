package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"redis-go/resp"
	"sync"
	"syscall"
	"time"
)

var aofWriter *bufio.Writer
var aofFile *os.File
var aofMu sync.RWMutex
var aofChan = make(chan []string, 20000) // 缓冲通道，防止阻塞

// 初始化AOF
func openAOF(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	aofFile = f
	aofWriter = bufio.NewWriter(aofFile)
	return nil
}

// 追加命令到channel
func appendAOF(cmd []string) {
	if aofWriter == nil {
		return
	}
	select {
	case aofChan <- cmd: // 非阻塞发送
	default:
		log.Println("aofChan is full, command discarded")
	}
}

// 加载时启用旧AOF
func loadAOF(path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}

	for len(data) > 0 {
		val, remain, err := resp.ParseRESP(data)
		if err != nil {
			return err
		}
		if val == nil {
			break
		}
		HandleCommand(val)
		data = remain
	}
	return nil
}

func bgReWriteAOF() {
	pid, _, _ := syscall.Syscall(syscall.SYS_FORK, 0, 0, 0)
	if pid == 0 {
		//子进程,遍历写temp-aof.aod
		temp, _ := os.Create("temp-aof.aof")
		w := bufio.NewWriter(temp)
		Store.SegScan(func(cmd []string) {
			arr := make(resp.Array, len(cmd))
			for i, c := range cmd {
				arr[i] = resp.BulkString(c)
			}
			w.Write(arr.ToBytes())
		})
		w.Flush()
		temp.Close()
		os.Exit(0)
	}
	//父进程,后台wait+rename
	go func() {
		syscall.Wait4(int(pid), nil, 0, nil)
		os.Rename("temp-aof.aof", "appendonly.aof")

		//aofMu.Lock()
		aofWriter.Flush()
		aofFile.Close()
		aofFile, _ = os.OpenFile("appendonly.aof", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		aofWriter = bufio.NewWriter(aofFile)
		//aofMu.Unlock()

	}()
}

// 启动AOF异步写入协程
func startAsyncAOF() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		batch := make([][]string, 0, 500)

		for {
			select {
			case cmd := <-aofChan:
				batch = append(batch, cmd)
				if len(batch) >= 500 {
					flushAOFBatch(batch)
					batch = batch[:0]
				}
			case <-ticker.C:
				if len(batch) > 0 {
					flushAOFBatch(batch)
					batch = batch[:0]
				}
			}
		}
	}()
}

func flushAOFBatch(batch [][]string) {
	go func() {
		// 非阻塞刷盘：即使刷盘慢，也不影响主请求
		tmpBuf := bytes.NewBuffer(nil)
		for _, cmd := range batch {
			arr := make(resp.Array, len(cmd))
			for i, c := range cmd {
				arr[i] = resp.BulkString(c)
			}
			tmpBuf.Write(arr.ToBytes())
		}
		aofMu.Lock()
		defer aofMu.Unlock()
		if _, err := aofWriter.Write(tmpBuf.Bytes()); err != nil {
			log.Println("flushAOFBatch write error:", err)
		}
		if err := aofWriter.Flush(); err != nil {
			log.Println("flushAOFBatch Flush error:", err)
		}
	}()
}
