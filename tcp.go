package riemanngo

import (
	"bytes"
	"encoding/binary"
	"net"
	"time"
)

type TCPClient struct {
	conn        net.Conn
	bf          *bufferedSender
	sendTimeout time.Duration
}

func NewTCPClient(addr string, connectTimeout, sendTimeout time.Duration) (*TCPClient, error) {
	conn, err := net.DialTimeout(
		"tcp", addr, connectTimeout,
	)

	if err != nil {
		return nil, err
	}

	c := &TCPClient{
		conn:        conn,
		bf:          newBufferedSender(),
		sendTimeout: sendTimeout,
	}

	c.bf.setSender(
		c.reallySend,
	)

	return c, nil
}

func (c *TCPClient) Send(e *Event) {
	c.bf.push(e)
}

func (c *TCPClient) Close() error {
	c.bf.stop()

	return c.conn.Close()
}

func (c *TCPClient) reallySend(data []byte) error {
	err := c.conn.SetDeadline(
		time.Now().Add(
			c.sendTimeout,
		),
	)

	if err != nil {
		return err
	}

	// ---

	size, err := msgSize(data)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(
		append(size, data...),
	)

	return err
}

func msgSize(data []byte) ([]byte, error) {
	buff := new(bytes.Buffer)

	err := binary.Write(
		buff,
		binary.BigEndian,
		uint32(len(data)),
	)

	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}
