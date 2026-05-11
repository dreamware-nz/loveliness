package transport

import (
	"bufio"
	"crypto/tls"
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
	shards    ShardQuerier
	listener  net.Listener
	tlsConfig *tls.Config
	wg        sync.WaitGroup
	stopCh    chan struct{}

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	stopped bool
}

// NewTCPServer creates a TCP transport server.
func NewTCPServer(shards ShardQuerier) *TCPServer {
	return &TCPServer{
		shards: shards,
		stopCh: make(chan struct{}),
		conns:  make(map[net.Conn]struct{}),
	}
}

// SetTLS configures mTLS for the transport listener.
func (s *TCPServer) SetTLS(cfg *tls.Config) {
	s.tlsConfig = cfg
}

// Listen starts accepting connections on the given address.
func (s *TCPServer) Listen(addr string) error {
	var ln net.Listener
	var err error
	if s.tlsConfig != nil {
		ln, err = tls.Listen("tcp", addr, s.tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
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

// Stop shuts down the TCP server gracefully. All live connections have their
// read deadlines pulled to now so in-flight handlers wake immediately rather
// than waiting up to 60s for the idle-read deadline to fire.
func (s *TCPServer) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		s.wg.Wait()
		return
	}
	s.stopped = true
	close(s.stopCh)
	live := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		live = append(live, c)
	}
	s.mu.Unlock()

	if s.listener != nil {
		s.listener.Close()
	}
	now := time.Now()
	for _, c := range live {
		// Ignore errors: conn may already be closed, which is fine.
		_ = c.SetReadDeadline(now)
	}
	s.wg.Wait()
}

// registerConn adds a connection to the live set. Returns false if the server
// has already stopped, in which case the caller must close the conn and exit.
func (s *TCPServer) registerConn(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

// deregisterConn removes a connection from the live set. Safe to call multiple
// times.
func (s *TCPServer) deregisterConn(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, c)
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
		// Register before spawning the handler so a concurrent Stop() that
		// races accept either tracks this conn (and pokes it) or sees stopped
		// and we close immediately.
		if !s.registerConn(conn) {
			conn.Close()
			continue
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	defer s.deregisterConn(conn)

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		msgType, payload, err := ReadFrame(reader)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Idle timeout — keep the connection alive, unless Stop() has
				// pulled the deadline to now, in which case the next loop
				// iteration sees stopCh closed and exits.
				continue
			}
			return // connection closed or broken
		}

		switch msgType {
		case MsgPing:
			_ = WriteFrame(writer, MsgPong, nil)
			_ = writer.Flush()

		case MsgQuery:
			var req QueryRequest
			if err := Decode(payload, &req); err != nil {
				resp := QueryResponse{Error: fmt.Sprintf("decode error: %v", err)}
				_ = WriteFrame(writer, MsgError, resp)
				_ = writer.Flush()
				continue
			}
			s.handleQuery(writer, &req)
			_ = writer.Flush()

		default:
			resp := QueryResponse{Error: fmt.Sprintf("unknown message type: %d", msgType)}
			_ = WriteFrame(writer, MsgError, resp)
			_ = writer.Flush()
		}
	}
}

func (s *TCPServer) handleQuery(w *bufio.Writer, req *QueryRequest) {
	sh := s.shards.GetShard(req.ShardID)
	if sh == nil {
		resp := QueryResponse{Error: fmt.Sprintf("shard %d not hosted on this node", req.ShardID)}
		_ = WriteFrame(w, MsgError, resp)
		return
	}

	result, err := sh.Query(req.Cypher)
	if err != nil {
		resp := QueryResponse{Error: err.Error()}
		_ = WriteFrame(w, MsgError, resp)
		return
	}

	resp := QueryResponse{
		Columns: result.Columns,
		Rows:    result.Rows,
	}
	resp.Stats.CompileTimeMs = result.Stats.CompileTimeMs
	resp.Stats.ExecTimeMs = result.Stats.ExecTimeMs
	_ = WriteFrame(w, MsgResult, resp)
}
