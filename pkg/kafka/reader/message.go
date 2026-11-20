package reader

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	kafkaPkg "github.com/segmentio/kafka-go"
)

// StableID follows the deduplication identity: a producer-supplied id header,
// or a hash of key and body when it is absent. It also survives DL forwarding.
func (m Message) StableID() string {
	if id, err := m.GetHeader("id"); err == nil {
		return "id:" + id
	}
	hash := sha256.New()
	_, _ = hash.Write(m.Key())
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(m.Body())
	return "content:" + hex.EncodeToString(hash.Sum(nil))
}

type Message struct {
	message kafkaPkg.Message
}

func NewMessage(message kafkaPkg.Message) *Message {
	return &Message{message: message}
}

func (m Message) RawMessage() kafkaPkg.Message {
	return m.message
}

func (m Message) Topic() string {
	return m.message.Topic
}

func (m Message) Body() []byte {
	return m.message.Value
}

func (m Message) Key() []byte {
	return m.message.Key
}

func (m Message) Partition() int {
	return m.message.Partition
}

func (m Message) Offset() int64 {
	return m.message.Offset
}

func (m Message) GetHeader(key string) (string, error) {
	for _, h := range m.message.Headers {
		if key == h.Key && string(h.Value) != "" {
			return string(h.Value), nil
		}
	}
	return "", fmt.Errorf("key %s is not found or value is empty", key)
}
