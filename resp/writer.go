package resp

import (
	"bytes"
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

type Null struct{}

func (n Null) ToBytes() []byte {
	return []byte("$-1\r\n")
}
