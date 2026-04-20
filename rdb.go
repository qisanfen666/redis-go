package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"log"
	"os"
	"redis-go/resp"
	"sync"
)

const rdbMagic = "REDIS0011"

var crcTable = crc64.MakeTable(crc64.ISO)

var saveMu sync.RWMutex

// bgsave入口
func bgsave() resp.RespValue {
	if !saveMu.TryLock() {
		return resp.Error("ERR Background save already in progress")
	}
	go func() {
		defer saveMu.Unlock()
		if err := doSave(); err != nil {
			log.Printf("BGSAVE failed: %v", err)
		}
	}()
	return resp.SimpleString("bgsave started")
}

// 执行RDB保存
func doSave() error {
	tmp := "dump.rdb.tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	crc := crc64.New(crcTable)

	// 写RDB文件头
	writeBytes(w, crc, []byte(rdbMagic))
	writeBytes(w, crc, []byte{0xFE, 0x00})

	Store.SegScan(func(cmd []string) {
		writeKey(w, crc, cmd[1], cmd[2])
	})
	writeBytes(w, crc, []byte{0xFF})
	_ = binary.Write(w, binary.LittleEndian, crc.Sum64())

	if err := w.Flush(); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return os.Rename(tmp, "dump.rdb")
}

func loadRDB() error {
	f, err := os.Open("dump.rdb")
	if os.IsNotExist(err) {
		return nil // 文件不存在，跳过加载
	}
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	// 读取并验证文件头
	magic := make([]byte, len(rdbMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != rdbMagic {
		return fmt.Errorf("invalid rdb file format")
	}

	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		switch b {
		case 0xFF: //EOF
			var crc uint64
			_ = binary.Read(r, binary.LittleEndian, &crc)
			return nil
		case 0xFE: //DB选择标记，跳过下一个字节
			_, _ = r.ReadByte()
			continue
		case 0x00: //string
			key, _ := readLenStr(r)
			val, _ := readLenStr(r)
			Store.Add(key, val)
		}
	}
}

// 工具函数
func writeBytes(w *bufio.Writer, crc hash.Hash64, p []byte) {
	w.Write(p)
	crc.Write(p)
}

func writeKey(w *bufio.Writer, crc hash.Hash64, key string, val string) {
	writeBytes(w, crc, []byte{0x00})
	writeLenStr(w, crc, key)
	writeLenStr(w, crc, val)
}

func writeLenStr(w *bufio.Writer, crc hash.Hash64, s string) {
	writeBytes(w, crc, []byte{byte(len(s))})
	writeBytes(w, crc, []byte(s))
}

func readLenStr(r *bufio.Reader) (string, error) {
	n, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	_, err = io.ReadFull(r, buf)

	return string(buf), err
}
