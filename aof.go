package main

import (
	"bufio"
	"io"
	"os"
	"redis-go/resp"
	"sync"
	"syscall"
	"time"
)

var aofWriter *bufio.Writer
var aofFile *os.File
var aofMu sync.Mutex

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

// 追加命令到AOF
func appendAOF(cmd []string) {
	if aofWriter == nil {
		return
	}
	arr := make(resp.Array, len(cmd))
	for i, c := range cmd {
		arr[i] = resp.BulkString(c)
	}
	buf := arr.ToBytes()

	aofMu.Lock()
	aofWriter.Write(buf)
	aofWriter.Flush()
	aofMu.Unlock()
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

		aofMu.Lock()
		aofWriter.Flush()
		aofFile.Close()
		aofFile, _ = os.OpenFile("appendonly.aof", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		aofWriter = bufio.NewWriter(aofFile)
		aofMu.Unlock()

	}()
}

func startAOFSync() {
	go func() {
		for {
			time.Sleep(time.Second)
			aofMu.Lock()
			aofWriter.Flush()
			aofFile.Sync()
			aofMu.Unlock()
		}
	}()
}
