package resp

import (
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []RespValue{
		SimpleString("OK"),
		Error("ERR unknown"),
		Integer(123),
		BulkString("hello"),
		Array{BulkString("SET"), BulkString("key"), BulkString("value")},
	}
	for _, v := range cases {
		enc := v.ToBytes()
		dec, left, err := ParseRESP(enc)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if len(left) != 0 {
			t.Fatalf("trailing data: %q", left)
		}
		// 深度比较
		if !deepEqual(dec, v) {
			t.Fatalf("not equal:\ngot  %#v\nwant %#v", dec, v)
		}
	}
}

// 简单深度比较辅助函数
func deepEqual(a, b RespValue) bool {
	switch va := a.(type) {
	case SimpleString:
		return va == b.(SimpleString)
	case Error:
		return va == b.(Error)
	case Integer:
		return va == b.(Integer)
	case BulkString:
		return va == b.(BulkString)
	case Array:
		vb := b.(Array)
		if len(va) != len(vb) {
			return false
		}
		for i := range va {
			if !deepEqual(va[i], vb[i]) {
				return false
			}
		}
		return true
	}
	return false
}
