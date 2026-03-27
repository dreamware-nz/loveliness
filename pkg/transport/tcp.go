package transport

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// ShardQuerier resolves a shard by ID and returns it for querying.
type ShardQuerier interface {
	GetShard(id int) *shard.Shard
}

// TCPServer listens for msgpack-over-TCP connections from peer nodes.
type TCPServer struct {
	shards   ShardQuerier
	listener net.Listener
	wg       sync.WaitGroup
	stopCh   chan struct{}
}

// NewTCPServer creates a TCP transport server.
func NewTCPServer(shards ShardQuerier) *TCPServer {
	return &TCPServer{
		shards: shards,
		stopCh: make(chan struct{}),
	}
}

// Listen starts accepting connections on the given address.
func (s *TCPServer) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp listen: %w", err)
	}
	s.listener = ln
	slog.Info("tcp transport listening", "addr", ln.Addr().String())

	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

// Addr returns the listener address (useful when binding to :0 in tests).
func (s *TCPServer) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Stop shuts down the TCP server gracefully.
func (s *TCPServer) Stop() {
	close(s.stopCh)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

func (s *TCPServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				slog.Error("tcp accept error", "err", err)
				continue
			}
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		msgType, payload, err := ReadFrame(reader)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue // idle timeout, keep connection alive
			}
			return // connection closed or broken
		}

		switch msgType {
		case MsgPing:
			WriteFrame(writer, MsgPong, nil)
			writer.Flush()

		case MsgQuery:
			var req QueryRequest
			if err := Decode(payload, &req); err != nil {
				resp := QueryResponse{Error: fmt.Sprintf("decode error: %v", err)}
				WriteFrame(writer, MsgError, resp)
				writer.Flush()
				continue
			}
			s.handleQuery(writer, &req)
			writer.Flush()

		default:
			resp := QueryResponse{Error: fmt.Sprintf("unknown message type: %d", msgType)}
			WriteFrame(writer, MsgError, resp)
			writer.Flush()
		}
	}
}

func (s *TCPServer) handleQuery(w *bufio.Writer, req *QueryRequest) {
	sh := s.shards.GetShard(req.ShardID)
	if sh == nil {
		resp := QueryResponse{Error: fmt.Sprintf("shard %d not hosted on this node", req.ShardID)}
		WriteFrame(w, MsgError, resp)
		return
	}

	result, err := sh.Query(req.Cypher)
	if err != nil {
		resp := QueryResponse{Error: err.Error()}
		WriteFrame(w, MsgError, resp)
		return
	}

	resp := QueryResponse{
		Columns: result.Columns,
		Rows:    result.Rows,
	}
	resp.Stats.CompileTimeMs = result.Stats.CompileTimeMs
	resp.Stats.ExecTimeMs = result.Stats.ExecTimeMs
	WriteFrame(w, MsgResult, resp)
}
