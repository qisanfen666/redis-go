package resp

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

func readUntilCRLF(data []byte) ([]byte, []byte, error) {
	idx := bytes.Index(data, []byte("\r\n"))
	if idx == -1 {
		return nil, data, errors.New("missing CRLF")
	}
	return data[:idx], data[idx+2:], nil
}

func ParseRESP(data []byte) (RespValue, []byte, error) {
	if len(data) == 0 {
		return nil, data, errors.New("empty data")
	}
	switch data[0] {
	case '+':
		str, rest, err := readUntilCRLF(data[1:])
		return SimpleString(str), rest, err
	case '-':
		str, rest, err := readUntilCRLF(data[1:])
		return Error(str), rest, err
	case ':':
		str, rest, err := readUntilCRLF(data[1:])
		num, _ := strconv.ParseInt(string(str), 10, 64)
		return Integer(num), rest, err
	case '$':
		lenStr, rest, err := readUntilCRLF(data[1:])
		length, _ := strconv.Atoi(string(lenStr))
		if length == -1 {
			return Null{}, rest, nil
		}
		if len(rest) < length+2 {
			return nil, data, errors.New("insufficient data")
		}
		content := rest[:length]
		return BulkString(content), rest[length+2:], err
	case '*':
		countStr, rest, err := readUntilCRLF(data[1:])
		count, _ := strconv.Atoi(string(countStr))
		arr := make(Array, 0, count)
		currentData := rest

		for i := 0; i < count; i++ {
			elem, newRest, err := ParseRESP(currentData)
			if err != nil {
				return nil, data, err
			}
			arr = append(arr, elem)
			currentData = newRest
		}
		return arr, currentData, err
	default:
		return nil, data, fmt.Errorf("unknown type: %c", data[0])
	}
}
