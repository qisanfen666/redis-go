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

const (
	rdbMagic   = "REDIS0009"
	rdbVersion = uint(9)
)

var crcTable = crc64.MakeTable(crc64.ISO)
var saveMu sync.Mutex

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

func doSave() error {
	tmp := "dump.rdb.tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	crc := crc64.New(crcTable)

	// 写MAGIC
	writeBytes(w, crc, []byte(rdbMagic))

	// 写RDB版本号
	binary.Write(w, binary.LittleEndian, rdbVersion)
	crc.Write([]byte{0, 0})

	// 写RDB数据库选择标记
	writeBytes(w, crc, []byte{0xFE, 0x00})

	// 写kv
	Store.Scan(func(key, value string) {
		writeString(w, crc, key)
		writeString(w, crc, value)
	})

	// 写EOF
	writeBytes(w, crc, []byte{0xFF})
	_ = binary.Write(w, binary.LittleEndian, crc.Sum64())

	// 刷新缓冲区
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	f.Close()

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

	// 读MAGIC
	magic := make([]byte, 9)
	if _, err := io.ReadFull(r, magic); err != nil {
		return fmt.Errorf("invalid rdb ")
	}
	if string(magic) != rdbMagic {
		return fmt.Errorf("invalid rdb magic: %s", string(magic))
	}

	// 读RDB版本号
	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("invalid rdb version: %v", err)
	}
	log.Printf("load rdb version: %d", version)

	for {
		opcode, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch opcode {
		case 0xFF:
			return nil
		case 0xFE:
			dbNum, err := r.ReadByte()
			if err != nil {
				return err
			}
			log.Printf("load rdb dbNum: %d", dbNum)
		default:
			if opcode != 0x00 {
				log.Printf("unknown opcode: %x", opcode)
				continue
			}

			key, err := readString(r)
			if err != nil {
				return err
			}
			val, err := readString(r)
			if err != nil {
				return err
			}
			Store.Set(key, val)
		}
	}

	return nil
}

func writeBytes(w *bufio.Writer, crc hash.Hash64, b []byte) {
	w.Write(b)
	crc.Write(b)
}

func writeString(w *bufio.Writer, crc hash.Hash64, s string) {
	// 写字符串长度
	writeBytes(w, crc, []byte{byte(len(s))})
	// 写字符串内容
	writeBytes(w, crc, []byte(s))
}

func readString(r *bufio.Reader) (string, error) {
	// 读字符串长度
	len, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	// 读字符串内容
	s := make([]byte, int(len))
	if _, err := io.ReadFull(r, s); err != nil {
		return "", err
	}
	return string(s), nil
}
