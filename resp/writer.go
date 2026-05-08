package resp

import (
	"bytes"
	"fmt"
	"strconv"
)

type RespValue interface {
	ToBytes() []byte
}

type SimpleString string

func (s SimpleString) ToBytes() []byte {
	return []byte("+" + string(s) + "\r\n")
}

type Error string

func (e Error) ToBytes() []byte {
	return []byte("-" + string(e) + "\r\n")
}

type Integer int64

func (i Integer) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(int64(i), 10) + "\r\n")
}

type BulkString string

func (b BulkString) ToBytes() []byte {
	if len(b) == 0 {
		return []byte("$-1\r\n")
	}
	return []byte("$" + strconv.Itoa(len(b)) + "\r\n" + string(b) + "\r\n")
}

type Array []RespValue

func (a Array) ToBytes() []byte {
	var buf bytes.Buffer
	buf.WriteByte('*')
	buf.WriteString(strconv.Itoa(len(a)))
	buf.WriteString("\r\n")

	for _, v := range a {
		buf.Write(v.ToBytes())
	}

	return buf.Bytes()
}

func (a *Array) Reset() {
	*a = (*a)[:0] // 截断切片长度为 0，底层数组不变
}

func (a *Array) UnmarshalBinary(buf []byte) error {
	// 1. 用 ParseRESP 解析整个 RESP 值
	val, _, err := ParseRESP(buf)
	if err != nil {
		return err
	}

	// 2. 类型断言：确保解析结果是 Array
	arr, ok := val.(Array)
	if !ok {
		return fmt.Errorf("resp: expected array, got %T", val)
	}

	// 3. 将解析结果赋值给当前 Array 对象
	*a = arr
	return nil
}

type Null struct{}

func (n Null) ToBytes() []byte {
	return []byte("$-1\r\n")
}
