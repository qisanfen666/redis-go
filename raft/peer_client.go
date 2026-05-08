package raft

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"sync"
	"time"
)

type peerClient struct {
	addr string
	mu   sync.Mutex
	conn net.Conn
}

func newPeerClient(addr string) *peerClient {
	return &peerClient{addr: addr}
}

func (c *peerClient) getConn() (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn, nil
	}
	conn, err := net.DialTimeout("tcp", c.addr, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return c.conn, nil
}

// 写帧：4 字节长度（大端）+ JSON 体
func writeFrame(conn net.Conn, v interface{}) error {
	var buf []byte
	if b, err := json.Marshal(v); err != nil {
		return err
	} else {
		buf = b
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(buf)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(buf)
	return err
}

// 读帧：4 字节长度 + JSON 体
func readFrame(conn net.Conn, v interface{}) error {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(header)
	body := make([]byte, length)
	if _, err := io.ReadFull(conn, body); err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}

func (c *peerClient) call(method string, args, reply interface{}) error {
	conn, err := c.getConn()
	if err != nil {
		return err
	}
	if err := writeFrame(conn, args); err != nil {
		c.closeLocked()
		return err
	}
	if err := readFrame(conn, reply); err != nil {
		c.closeLocked()
		return err
	}
	return nil
}

func (c *peerClient) closeLocked() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
