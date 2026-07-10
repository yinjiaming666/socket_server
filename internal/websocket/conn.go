package websocket

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	// maxFramePayloadSize 限制单个 WebSocket 帧最大载荷，避免异常客户端打爆内存。
	maxFramePayloadSize = 1 << 20
	// websocketReadTimeout 表示单次读帧超时。
	websocketReadTimeout = 70 * time.Second
	// websocketWriteTimeout 表示单次写帧超时。
	websocketWriteTimeout = 10 * time.Second
)

// wsConn 表示一个最小 WebSocket 连接实现。
type wsConn struct {
	conn    net.Conn
	reader  *bufio.Reader
	writeMu sync.Mutex
}

// upgradeWebSocket 使用标准库完成 WebSocket 握手升级。
func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return nil, errors.New("not a websocket upgrade request")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("Sec-WebSocket-Key is empty")
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("http hijacker is not supported")
	}
	conn, readWriter, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}

	accept := websocketAccept(key)
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := conn.Write([]byte(response)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, reader: readWriter.Reader}, nil
}

// websocketAccept 生成 WebSocket 握手响应值。
func websocketAccept(key string) string {
	hash := sha1.Sum([]byte(key + websocketGUID))
	return base64.StdEncoding.EncodeToString(hash[:])
}

// readMessage 读取一个文本或二进制消息帧。
func (c *wsConn) readMessage() ([]byte, byte, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(websocketReadTimeout))

	header := make([]byte, 2)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return nil, 0, err
	}

	opcode := header[0] & 0x0F
	masked := header[1]&0x80 != 0
	payloadLen := uint64(header[1] & 0x7F)
	switch payloadLen {
	case 126:
		ext := make([]byte, 2)
		if _, err := io.ReadFull(c.reader, ext); err != nil {
			return nil, 0, err
		}
		payloadLen = uint64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := io.ReadFull(c.reader, ext); err != nil {
			return nil, 0, err
		}
		payloadLen = binary.BigEndian.Uint64(ext)
	}
	if payloadLen > maxFramePayloadSize {
		return nil, 0, errors.New("websocket frame is too large")
	}

	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(c.reader, maskKey[:]); err != nil {
			return nil, 0, err
		}
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return nil, 0, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return payload, opcode, nil
}

// writeText 写入一个服务端文本帧。
func (c *wsConn) writeText(payload []byte) error { return c.writeFrame(0x1, payload) }

// writePing 写入 ping 帧。
func (c *wsConn) writePing(payload []byte) error { return c.writeFrame(0x9, payload) }

// writePong 写入 pong 帧。
func (c *wsConn) writePong(payload []byte) error { return c.writeFrame(0xA, payload) }

// writeClose 写入 close 帧。
func (c *wsConn) writeClose() error { return c.writeFrame(0x8, nil) }

// writeFrame 写入一个未掩码服务端帧。
func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	_ = c.conn.SetWriteDeadline(time.Now().Add(websocketWriteTimeout))
	defer c.conn.SetWriteDeadline(time.Time{})

	header := []byte{0x80 | opcode}
	length := len(payload)
	switch {
	case length < 126:
		header = append(header, byte(length))
	case length <= 65535:
		header = append(header, 126, byte(length>>8), byte(length))
	default:
		header = append(header, 127)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(length))
		header = append(header, ext[:]...)
	}
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := c.conn.Write(payload)
	return err
}

// close 关闭底层连接。
func (c *wsConn) close() error { return c.conn.Close() }
