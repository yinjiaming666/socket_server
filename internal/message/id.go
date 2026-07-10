package message

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"
)

// EnsureMessageID 确保消息有唯一编号。
func EnsureMessageID(prefix string, messageID string) string {
	if messageID != "" {
		return messageID
	}
	if prefix == "" {
		prefix = "msg"
	}
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + hex.EncodeToString(buf[:])
}
