package transport

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgType identifies the kind of message on the wire.
type MsgType uint8

const (
	MsgQuery    MsgType = 1
	MsgResult   MsgType = 2
	MsgError    MsgType = 3
	MsgPing     MsgType = 10
	MsgPong     MsgType = 11
)

// Frame is the wire format: [4 bytes length][1 byte type][msgpack payload].
const frameHeaderSize = 5 // 4 (length) + 1 (type)

// WriteFrame writes a length-prefixed, typed msgpack message to w.
func WriteFrame(w io.Writer, msgType MsgType, v any) error {
	payload, err := msgpack.Marshal(v)
	if err != nil {
		return fmt.Errorf("msgpack marshal: %w", err)
	}

	// Header: 4-byte big-endian length (of type + payload), then 1-byte type.
	totalLen := uint32(1 + len(payload))
	var hdr [frameHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[:4], totalLen)
	hdr[4] = byte(msgType)

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

// ReadFrame reads a length-prefixed, typed msgpack message from r.
// Returns the message type and raw payload bytes.
func ReadFrame(r io.Reader) (MsgType, []byte, error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}

	totalLen := binary.BigEndian.Uint32(hdr[:4])
	if totalLen < 1 {
		return 0, nil, fmt.Errorf("invalid frame length: %d", totalLen)
	}
	if totalLen > 64<<20 { // 64 MB max frame
		return 0, nil, fmt.Errorf("frame too large: %d bytes", totalLen)
	}

	msgType := MsgType(hdr[4])
	payloadLen := totalLen - 1
	if payloadLen == 0 {
		return msgType, nil, nil
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

// Decode unmarshals msgpack payload into v.
func Decode(payload []byte, v any) error {
	return msgpack.Unmarshal(payload, v)
}
