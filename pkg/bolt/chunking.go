package bolt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Bolt uses chunked transfer: each message is split into chunks of up to
// 65535 bytes. A zero-length chunk (0x00 0x00) marks the end of a message.
//
// Wire format:
//   [2-byte chunk size][chunk data]
//   [2-byte chunk size][chunk data]
//   [0x00 0x00]  ← end of message

const maxChunkSize = 0xFFFF

// WriteMessage writes a PackStream-encoded message as Bolt chunks.
func WriteMessage(w io.Writer, data []byte) error {
	for len(data) > 0 {
		chunk := data
		if len(chunk) > maxChunkSize {
			chunk = data[:maxChunkSize]
		}
		data = data[len(chunk):]

		// Write chunk header (2-byte big-endian size).
		header := [2]byte{}
		binary.BigEndian.PutUint16(header[:], uint16(len(chunk)))
		if _, err := w.Write(header[:]); err != nil {
			return err
		}
		// Write chunk data.
		if _, err := w.Write(chunk); err != nil {
			return err
		}
	}

	// End-of-message marker.
	if _, err := w.Write([]byte{0x00, 0x00}); err != nil {
		return err
	}
	return nil
}

// ReadMessage reads a complete chunked Bolt message and returns the
// reassembled PackStream data.
func ReadMessage(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	header := [2]byte{}

	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return nil, err
		}
		size := binary.BigEndian.Uint16(header[:])
		if size == 0 {
			break // end of message
		}
		chunk := make([]byte, size)
		if _, err := io.ReadFull(r, chunk); err != nil {
			return nil, err
		}
		buf.Write(chunk)
	}

	if buf.Len() == 0 {
		return nil, fmt.Errorf("bolt: empty message")
	}
	return buf.Bytes(), nil
}
