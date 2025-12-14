package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"redis-go/resp"
	"strconv"
	"sync"
	"time"
)

const aofFileName = "appendonly.aof"

var (
	aofMu    sync.RWMutex
	aofFile  *os.File
	aofBuf   *bufio.Writer
	aofState int32  = 1
	fsPolicy string = "everysec"
)

func initAOF() error {
	aofMu.Lock()
	defer aofMu.Unlock()

	f, err := os.OpenFile(aofFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	aofFile = f
	aofBuf = bufio.NewWriter(f)
	return nil
}

func closeAOF() {
	aofMu.Lock()
	defer aofMu.Unlock()

	if aofBuf != nil {
		aofBuf.Flush()
	}
	if aofFile != nil {
		aofFile.Sync()
		aofFile.Close()
	}
}

func appendAOF(cmd []string) {
	if aofState == 0 {
		return
	}
	aofMu.Lock()
	defer aofMu.Unlock()

	aofBuf.WriteByte('*')
	aofBuf.WriteString(strconv.Itoa(len(cmd)))
	aofBuf.WriteString("\r\n")
	for _, arg := range cmd {
		aofBuf.WriteByte('$')
		aofBuf.WriteString(strconv.Itoa(len(arg)))
		aofBuf.WriteString("\r\n")
		aofBuf.WriteString(arg)
		aofBuf.WriteString("\r\n")
	}

}

func aofFsyncEverySec() {
	if fsPolicy != "everysec" {
		return
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for range tick.C {
		aofMu.Lock()
		aofBuf.Flush()
		aofFile.Sync()
		aofMu.Unlock()
	}
}

func bgReWriteAOF() {
	log.Println("bgReWriteAOF start")

	temp := aofFileName + ".tmp"
	f, err := os.Create(temp)
	if err != nil {
		log.Println("bgReWriteAOF create temp file error:", err)
		return
	}
	w := bufio.NewWriter(f)

	Store.SegScan(func(cmd []string) {
		writeRespArray(w, cmd)
	})

	w.Flush()
	f.Sync()
	f.Close()

	os.Rename(temp, aofFileName)
	log.Println("bgReWriteAOF done")
}

func loadAOF() error {
	f, err := os.Open(aofFileName)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

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

		val, _, err := resp.ParseRESP(append([]byte{}, raw...))
		if err != nil {
			log.Printf("[AOF] ParseRESP error: %v", err)
			continue
		}
		HandleCommand(val, nil)
	}
	return nil
}

func writeRespArray(w *bufio.Writer, args []string) {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(args)))
	w.WriteString("\r\n")
	for _, s := range args {
		w.WriteByte('$')
		w.WriteString(strconv.Itoa(len(s)))
		w.WriteString("\r\n")
		w.WriteString(s)
		w.WriteString("\r\n")
	}
}
