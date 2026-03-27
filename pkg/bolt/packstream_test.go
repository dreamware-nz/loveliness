package bolt

import (
	"bytes"
	"math"
	"testing"
)

func TestPackUnpackNull(t *testing.T) {
	p := NewPacker()
	p.PackNull()

	u := NewUnpacker(bytes.NewReader(p.Bytes()))
	v, err := u.Unpack()
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Errorf("expected nil, got %v", v)
	}
}

func TestPackUnpackBool(t *testing.T) {
	for _, expected := range []bool{true, false} {
		p := NewPacker()
		p.PackBool(expected)

		u := NewUnpacker(bytes.NewReader(p.Bytes()))
		v, err := u.Unpack()
		if err != nil {
			t.Fatal(err)
		}
		if v != expected {
			t.Errorf("expected %v, got %v", expected, v)
		}
	}
}

func TestPackUnpackIntegers(t *testing.T) {
	cases := []int64{
		0, 1, -1, 42, -16, 127, -128,
		128, 32767, -32768,
		32768, 2147483647, -2147483648,
		2147483648, math.MaxInt64, math.MinInt64,
	}
	for _, expected := range cases {
		p := NewPacker()
		p.PackInt(expected)

		u := NewUnpacker(bytes.NewReader(p.Bytes()))
		v, err := u.Unpack()
		if err != nil {
			t.Fatalf("int %d: %v", expected, err)
		}
		got, ok := v.(int64)
		if !ok {
			t.Fatalf("int %d: expected int64, got %T", expected, v)
		}
		if got != expected {
			t.Errorf("expected %d, got %d", expected, got)
		}
	}
}

func TestPackUnpackFloat(t *testing.T) {
	expected := 3.14159
	p := NewPacker()
	p.PackFloat(expected)

	u := NewUnpacker(bytes.NewReader(p.Bytes()))
	v, err := u.Unpack()
	if err != nil {
		t.Fatal(err)
	}
	got, ok := v.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", v)
	}
	if got != expected {
		t.Errorf("expected %f, got %f", expected, got)
	}
}

func TestPackUnpackString(t *testing.T) {
	cases := []string{
		"", "hello", "a", string(make([]byte, 200)), string(make([]byte, 70000)),
	}
	for _, expected := range cases {
		p := NewPacker()
		p.PackString(expected)

		u := NewUnpacker(bytes.NewReader(p.Bytes()))
		v, err := u.Unpack()
		if err != nil {
			t.Fatalf("string len %d: %v", len(expected), err)
		}
		got, ok := v.(string)
		if !ok {
			t.Fatalf("expected string, got %T", v)
		}
		if got != expected {
			t.Errorf("string mismatch (len %d vs %d)", len(expected), len(got))
		}
	}
}

func TestPackUnpackList(t *testing.T) {
	p := NewPacker()
	p.PackListHeader(3)
	p.PackInt(1)
	p.PackString("two")
	p.PackBool(true)

	u := NewUnpacker(bytes.NewReader(p.Bytes()))
	v, err := u.Unpack()
	if err != nil {
		t.Fatal(err)
	}
	list, ok := v.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", v)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 items, got %d", len(list))
	}
	if list[0].(int64) != 1 {
		t.Error("item 0 mismatch")
	}
	if list[1].(string) != "two" {
		t.Error("item 1 mismatch")
	}
	if list[2].(bool) != true {
		t.Error("item 2 mismatch")
	}
}

func TestPackUnpackMap(t *testing.T) {
	p := NewPacker()
	p.PackMapHeader(2)
	p.PackString("name")
	p.PackString("Alice")
	p.PackString("age")
	p.PackInt(30)

	u := NewUnpacker(bytes.NewReader(p.Bytes()))
	v, err := u.Unpack()
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", v)
	}
	if m["name"].(string) != "Alice" {
		t.Error("name mismatch")
	}
	if m["age"].(int64) != 30 {
		t.Error("age mismatch")
	}
}

func TestPackUnpackStruct(t *testing.T) {
	p := NewPacker()
	p.PackStructHeader(2, 0x42)
	p.PackString("hello")
	p.PackInt(99)

	u := NewUnpacker(bytes.NewReader(p.Bytes()))
	v, err := u.Unpack()
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.(*BoltStruct)
	if !ok {
		t.Fatalf("expected *BoltStruct, got %T", v)
	}
	if s.Tag != 0x42 {
		t.Errorf("expected tag 0x42, got 0x%02X", s.Tag)
	}
	if len(s.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(s.Fields))
	}
}

func TestPackUnpackBytes(t *testing.T) {
	expected := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	p := NewPacker()
	p.PackBytes(expected)

	u := NewUnpacker(bytes.NewReader(p.Bytes()))
	v, err := u.Unpack()
	if err != nil {
		t.Fatal(err)
	}
	got, ok := v.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", v)
	}
	if !bytes.Equal(got, expected) {
		t.Error("bytes mismatch")
	}
}

func TestPackValueAutoType(t *testing.T) {
	p := NewPacker()
	p.PackValue(nil)
	p.PackValue(true)
	p.PackValue(int64(42))
	p.PackValue("hello")
	p.PackValue(3.14)
	p.PackValue([]any{int64(1), "two"})
	p.PackValue(map[string]any{"key": "val"})

	u := NewUnpacker(bytes.NewReader(p.Bytes()))

	// null
	v, _ := u.Unpack()
	if v != nil {
		t.Error("expected nil")
	}
	// true
	v, _ = u.Unpack()
	if v != true {
		t.Error("expected true")
	}
	// 42
	v, _ = u.Unpack()
	if v.(int64) != 42 {
		t.Error("expected 42")
	}
	// "hello"
	v, _ = u.Unpack()
	if v.(string) != "hello" {
		t.Error("expected hello")
	}
	// 3.14
	v, _ = u.Unpack()
	if v.(float64) != 3.14 {
		t.Error("expected 3.14")
	}
	// list
	v, _ = u.Unpack()
	list := v.([]any)
	if len(list) != 2 {
		t.Error("expected 2-element list")
	}
	// map
	v, _ = u.Unpack()
	m := v.(map[string]any)
	if m["key"].(string) != "val" {
		t.Error("expected key=val")
	}
}

func TestTinyIntEncoding(t *testing.T) {
	// Tiny positive ints (0-127) should be a single byte.
	p := NewPacker()
	p.PackInt(42)
	if len(p.Bytes()) != 1 {
		t.Errorf("tiny int 42 should be 1 byte, got %d", len(p.Bytes()))
	}

	// Tiny negative ints (-16 to -1) should be a single byte.
	p.Reset()
	p.PackInt(-1)
	if len(p.Bytes()) != 1 {
		t.Errorf("tiny int -1 should be 1 byte, got %d", len(p.Bytes()))
	}
}
