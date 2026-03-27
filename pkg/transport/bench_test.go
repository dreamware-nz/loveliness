package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func makeTestResponse(rowCount int) QueryResponse {
	rows := make([]map[string]any, rowCount)
	for i := range rows {
		rows[i] = map[string]any{
			"name": fmt.Sprintf("Person-%d", i),
			"age":  int64(20 + i%60),
			"city": "Auckland",
		}
	}
	return QueryResponse{
		Columns: []string{"name", "age", "city"},
		Rows:    rows,
	}
}

func BenchmarkJSONMarshal_10rows(b *testing.B) {
	resp := makeTestResponse(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Marshal(resp)
	}
}

func BenchmarkMsgpackMarshal_10rows(b *testing.B) {
	resp := makeTestResponse(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpack.Marshal(resp)
	}
}

func BenchmarkJSONMarshal_1000rows(b *testing.B) {
	resp := makeTestResponse(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Marshal(resp)
	}
}

func BenchmarkMsgpackMarshal_1000rows(b *testing.B) {
	resp := makeTestResponse(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgpack.Marshal(resp)
	}
}

func BenchmarkJSONUnmarshal_1000rows(b *testing.B) {
	resp := makeTestResponse(1000)
	data, _ := json.Marshal(resp)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out QueryResponse
		json.Unmarshal(data, &out)
	}
}

func BenchmarkMsgpackUnmarshal_1000rows(b *testing.B) {
	resp := makeTestResponse(1000)
	data, _ := msgpack.Marshal(resp)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out QueryResponse
		msgpack.Unmarshal(data, &out)
	}
}

func BenchmarkJSONRoundtrip_1000rows(b *testing.B) {
	resp := makeTestResponse(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, _ := json.Marshal(resp)
		var out QueryResponse
		json.Unmarshal(data, &out)
	}
}

func BenchmarkMsgpackRoundtrip_1000rows(b *testing.B) {
	resp := makeTestResponse(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, _ := msgpack.Marshal(resp)
		var out QueryResponse
		msgpack.Unmarshal(data, &out)
	}
}

func BenchmarkFrameRoundtrip_1000rows(b *testing.B) {
	resp := makeTestResponse(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		WriteFrame(&buf, MsgResult, resp)
		ReadFrame(&buf)
	}
}

func BenchmarkHTTPJSON_1000rows(b *testing.B) {
	// Simulates what the HTTP transport does: marshal request + unmarshal response.
	req := QueryRequest{ShardID: 0, Cypher: "MATCH (n) RETURN n"}
	resp := makeTestResponse(1000)
	respData, _ := json.Marshal(resp)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Marshal(req)
		var out QueryResponse
		json.Unmarshal(respData, &out)
	}
}

func BenchmarkTCPMsgpack_1000rows(b *testing.B) {
	// Simulates what the TCP transport does: frame request + decode response.
	req := QueryRequest{ShardID: 0, Cypher: "MATCH (n) RETURN n"}
	resp := makeTestResponse(1000)
	var respBuf bytes.Buffer
	WriteFrame(&respBuf, MsgResult, resp)
	respBytes := respBuf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wbuf bytes.Buffer
		WriteFrame(&wbuf, MsgQuery, req)
		rbuf := bytes.NewReader(respBytes)
		_, payload, _ := ReadFrame(rbuf)
		var out QueryResponse
		Decode(payload, &out)
	}
}
