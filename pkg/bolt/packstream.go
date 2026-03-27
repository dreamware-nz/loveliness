package bolt

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// PackStream implements Neo4j's PackStream binary serialization format.
// It's similar to MessagePack but adds struct types for graph objects
// (Node, Relationship, Path, etc.).
//
// Reference: https://neo4j.com/docs/bolt/current/packstream/

// Marker bytes for PackStream types.
const (
	markerTinyString  = 0x80
	markerTinyList    = 0x90
	markerTinyMap     = 0xA0
	markerTinyStruct  = 0xB0
	markerNull        = 0xC0
	markerFloat64     = 0xC1
	markerFalse       = 0xC2
	markerTrue        = 0xC3
	markerInt8        = 0xC8
	markerInt16       = 0xC9
	markerInt32       = 0xCA
	markerInt64       = 0xCB
	markerString8     = 0xD0
	markerString16    = 0xD1
	markerString32    = 0xD2
	markerBytes8      = 0xCC
	markerBytes16     = 0xCD
	markerBytes32     = 0xCE
	markerList8       = 0xD4
	markerList16      = 0xD5
	markerList32      = 0xD6
	markerMap8        = 0xD8
	markerMap16       = 0xD9
	markerMap32       = 0xDA
	markerStruct8     = 0xDC
	markerStruct16    = 0xDD
)

// Struct tags for Bolt graph types.
const (
	tagNode         = 0x4E // 'N'
	tagRelationship = 0x52 // 'R'
	tagPath         = 0x50 // 'P'
	tagUnboundRel   = 0x72 // 'r'
)

// Packer writes PackStream-encoded values to a byte buffer.
type Packer struct {
	buf []byte
}

// NewPacker creates a new packer.
func NewPacker() *Packer {
	return &Packer{buf: make([]byte, 0, 512)}
}

// Bytes returns the packed data.
func (p *Packer) Bytes() []byte {
	return p.buf
}

// Reset clears the buffer for reuse.
func (p *Packer) Reset() {
	p.buf = p.buf[:0]
}

func (p *Packer) writeByte(b byte) {
	p.buf = append(p.buf, b)
}

func (p *Packer) writeBytes(b []byte) {
	p.buf = append(p.buf, b...)
}

// PackNull writes a null value.
func (p *Packer) PackNull() {
	p.writeByte(markerNull)
}

// PackBool writes a boolean value.
func (p *Packer) PackBool(v bool) {
	if v {
		p.writeByte(markerTrue)
	} else {
		p.writeByte(markerFalse)
	}
}

// PackInt writes an integer with the smallest possible encoding.
func (p *Packer) PackInt(v int64) {
	if v >= -16 && v <= 127 {
		// Tiny int: single byte.
		p.writeByte(byte(v))
	} else if v >= -128 && v <= 127 {
		p.writeByte(markerInt8)
		p.writeByte(byte(v))
	} else if v >= -32768 && v <= 32767 {
		p.writeByte(markerInt16)
		b := [2]byte{}
		binary.BigEndian.PutUint16(b[:], uint16(v))
		p.writeBytes(b[:])
	} else if v >= -2147483648 && v <= 2147483647 {
		p.writeByte(markerInt32)
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(v))
		p.writeBytes(b[:])
	} else {
		p.writeByte(markerInt64)
		b := [8]byte{}
		binary.BigEndian.PutUint64(b[:], uint64(v))
		p.writeBytes(b[:])
	}
}

// PackFloat writes a 64-bit float.
func (p *Packer) PackFloat(v float64) {
	p.writeByte(markerFloat64)
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
	p.writeBytes(b[:])
}

// PackString writes a UTF-8 string.
func (p *Packer) PackString(s string) {
	n := len(s)
	if n < 16 {
		p.writeByte(byte(markerTinyString | n))
	} else if n <= 0xFF {
		p.writeByte(markerString8)
		p.writeByte(byte(n))
	} else if n <= 0xFFFF {
		p.writeByte(markerString16)
		b := [2]byte{}
		binary.BigEndian.PutUint16(b[:], uint16(n))
		p.writeBytes(b[:])
	} else {
		p.writeByte(markerString32)
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(n))
		p.writeBytes(b[:])
	}
	p.writeBytes([]byte(s))
}

// PackBytes writes a byte array.
func (p *Packer) PackBytes(data []byte) {
	n := len(data)
	if n <= 0xFF {
		p.writeByte(markerBytes8)
		p.writeByte(byte(n))
	} else if n <= 0xFFFF {
		p.writeByte(markerBytes16)
		b := [2]byte{}
		binary.BigEndian.PutUint16(b[:], uint16(n))
		p.writeBytes(b[:])
	} else {
		p.writeByte(markerBytes32)
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(n))
		p.writeBytes(b[:])
	}
	p.writeBytes(data)
}

// PackListHeader writes the header for a list of n elements.
// The caller must pack exactly n values after this.
func (p *Packer) PackListHeader(n int) {
	if n < 16 {
		p.writeByte(byte(markerTinyList | n))
	} else if n <= 0xFF {
		p.writeByte(markerList8)
		p.writeByte(byte(n))
	} else if n <= 0xFFFF {
		p.writeByte(markerList16)
		b := [2]byte{}
		binary.BigEndian.PutUint16(b[:], uint16(n))
		p.writeBytes(b[:])
	} else {
		p.writeByte(markerList32)
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(n))
		p.writeBytes(b[:])
	}
}

// PackMapHeader writes the header for a map of n key-value pairs.
// The caller must pack exactly n key-value pairs after this.
func (p *Packer) PackMapHeader(n int) {
	if n < 16 {
		p.writeByte(byte(markerTinyMap | n))
	} else if n <= 0xFF {
		p.writeByte(markerMap8)
		p.writeByte(byte(n))
	} else if n <= 0xFFFF {
		p.writeByte(markerMap16)
		b := [2]byte{}
		binary.BigEndian.PutUint16(b[:], uint16(n))
		p.writeBytes(b[:])
	} else {
		p.writeByte(markerMap32)
		b := [4]byte{}
		binary.BigEndian.PutUint32(b[:], uint32(n))
		p.writeBytes(b[:])
	}
}

// PackStructHeader writes a struct marker with tag and field count.
func (p *Packer) PackStructHeader(fields int, tag byte) {
	if fields < 16 {
		p.writeByte(byte(markerTinyStruct | fields))
		p.writeByte(tag)
	} else if fields <= 0xFF {
		p.writeByte(markerStruct8)
		p.writeByte(byte(fields))
		p.writeByte(tag)
	} else {
		p.writeByte(markerStruct16)
		b := [2]byte{}
		binary.BigEndian.PutUint16(b[:], uint16(fields))
		p.writeBytes(b[:])
		p.writeByte(tag)
	}
}

// PackValue packs an arbitrary Go value.
func (p *Packer) PackValue(v any) {
	if v == nil {
		p.PackNull()
		return
	}
	switch val := v.(type) {
	case bool:
		p.PackBool(val)
	case int:
		p.PackInt(int64(val))
	case int8:
		p.PackInt(int64(val))
	case int16:
		p.PackInt(int64(val))
	case int32:
		p.PackInt(int64(val))
	case int64:
		p.PackInt(val)
	case float32:
		p.PackFloat(float64(val))
	case float64:
		// Check if it's actually an integer (JSON decode quirk).
		if val == math.Trunc(val) && val >= math.MinInt64 && val <= math.MaxInt64 {
			p.PackInt(int64(val))
		} else {
			p.PackFloat(val)
		}
	case string:
		p.PackString(val)
	case []byte:
		p.PackBytes(val)
	case []any:
		p.PackListHeader(len(val))
		for _, item := range val {
			p.PackValue(item)
		}
	case map[string]any:
		p.PackMapHeader(len(val))
		for k, v := range val {
			p.PackString(k)
			p.PackValue(v)
		}
	default:
		// Fallback: convert to string.
		p.PackString(fmt.Sprintf("%v", val))
	}
}

// Unpacker reads PackStream-encoded data from a byte reader.
type Unpacker struct {
	r io.Reader
}

// NewUnpacker creates a new unpacker.
func NewUnpacker(r io.Reader) *Unpacker {
	return &Unpacker{r: r}
}

func (u *Unpacker) readByte() (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(u.r, b[:])
	return b[0], err
}

func (u *Unpacker) readN(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(u.r, b)
	return b, err
}

func (u *Unpacker) readUint16() (uint16, error) {
	b, err := u.readN(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (u *Unpacker) readUint32() (uint32, error) {
	b, err := u.readN(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

// Unpack reads and returns the next PackStream value.
func (u *Unpacker) Unpack() (any, error) {
	marker, err := u.readByte()
	if err != nil {
		return nil, err
	}

	high := marker & 0xF0

	// Tiny int (positive): 0x00..0x7F
	if marker <= 0x7F {
		return int64(marker), nil
	}

	// Tiny int (negative): 0xF0..0xFF
	if marker >= 0xF0 {
		return int64(int8(marker)), nil
	}

	// Tiny string: 0x80..0x8F
	if high == markerTinyString {
		n := int(marker & 0x0F)
		b, err := u.readN(n)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	}

	// Tiny list: 0x90..0x9F
	if high == markerTinyList {
		return u.unpackList(int(marker & 0x0F))
	}

	// Tiny map: 0xA0..0xAF
	if high == markerTinyMap {
		return u.unpackMap(int(marker & 0x0F))
	}

	// Tiny struct: 0xB0..0xBF
	if high == markerTinyStruct {
		return u.unpackStruct(int(marker & 0x0F))
	}

	switch marker {
	case markerNull:
		return nil, nil
	case markerTrue:
		return true, nil
	case markerFalse:
		return false, nil
	case markerFloat64:
		b, err := u.readN(8)
		if err != nil {
			return nil, err
		}
		bits := binary.BigEndian.Uint64(b)
		return math.Float64frombits(bits), nil
	case markerInt8:
		b, err := u.readByte()
		if err != nil {
			return nil, err
		}
		return int64(int8(b)), nil
	case markerInt16:
		v, err := u.readUint16()
		if err != nil {
			return nil, err
		}
		return int64(int16(v)), nil
	case markerInt32:
		v, err := u.readUint32()
		if err != nil {
			return nil, err
		}
		return int64(int32(v)), nil
	case markerInt64:
		b, err := u.readN(8)
		if err != nil {
			return nil, err
		}
		return int64(binary.BigEndian.Uint64(b)), nil
	case markerString8:
		n, err := u.readByte()
		if err != nil {
			return nil, err
		}
		b, err := u.readN(int(n))
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case markerString16:
		n, err := u.readUint16()
		if err != nil {
			return nil, err
		}
		b, err := u.readN(int(n))
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case markerString32:
		n, err := u.readUint32()
		if err != nil {
			return nil, err
		}
		b, err := u.readN(int(n))
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case markerBytes8:
		n, err := u.readByte()
		if err != nil {
			return nil, err
		}
		return u.readN(int(n))
	case markerBytes16:
		n, err := u.readUint16()
		if err != nil {
			return nil, err
		}
		return u.readN(int(n))
	case markerBytes32:
		n, err := u.readUint32()
		if err != nil {
			return nil, err
		}
		return u.readN(int(n))
	case markerList8:
		n, err := u.readByte()
		if err != nil {
			return nil, err
		}
		return u.unpackList(int(n))
	case markerList16:
		n, err := u.readUint16()
		if err != nil {
			return nil, err
		}
		return u.unpackList(int(n))
	case markerList32:
		n, err := u.readUint32()
		if err != nil {
			return nil, err
		}
		return u.unpackList(int(n))
	case markerMap8:
		n, err := u.readByte()
		if err != nil {
			return nil, err
		}
		return u.unpackMap(int(n))
	case markerMap16:
		n, err := u.readUint16()
		if err != nil {
			return nil, err
		}
		return u.unpackMap(int(n))
	case markerMap32:
		n, err := u.readUint32()
		if err != nil {
			return nil, err
		}
		return u.unpackMap(int(n))
	case markerStruct8:
		n, err := u.readByte()
		if err != nil {
			return nil, err
		}
		return u.unpackStruct(int(n))
	case markerStruct16:
		n, err := u.readUint16()
		if err != nil {
			return nil, err
		}
		return u.unpackStruct(int(n))
	}

	return nil, fmt.Errorf("packstream: unknown marker 0x%02X", marker)
}

func (u *Unpacker) unpackList(n int) ([]any, error) {
	list := make([]any, n)
	for i := 0; i < n; i++ {
		v, err := u.Unpack()
		if err != nil {
			return nil, err
		}
		list[i] = v
	}
	return list, nil
}

func (u *Unpacker) unpackMap(n int) (map[string]any, error) {
	m := make(map[string]any, n)
	for i := 0; i < n; i++ {
		key, err := u.Unpack()
		if err != nil {
			return nil, err
		}
		val, err := u.Unpack()
		if err != nil {
			return nil, err
		}
		keyStr, ok := key.(string)
		if !ok {
			keyStr = fmt.Sprintf("%v", key)
		}
		m[keyStr] = val
	}
	return m, nil
}

// BoltStruct represents a PackStream struct (used for Bolt messages and graph types).
type BoltStruct struct {
	Tag    byte
	Fields []any
}

func (u *Unpacker) unpackStruct(n int) (*BoltStruct, error) {
	tag, err := u.readByte()
	if err != nil {
		return nil, err
	}
	fields := make([]any, n)
	for i := 0; i < n; i++ {
		v, err := u.Unpack()
		if err != nil {
			return nil, err
		}
		fields[i] = v
	}
	return &BoltStruct{Tag: tag, Fields: fields}, nil
}
