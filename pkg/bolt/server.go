package bolt

import (
	"bytes"
	"crypto/subtle"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

// Bolt handshake magic preamble.
var boltMagic = []byte{0x60, 0x60, 0xB0, 0x17}

// QueryExecutor executes Cypher and returns results.
type QueryExecutor interface {
	Query(cypher string, params map[string]any) (columns []string, rows []map[string]any, err error)
}

// Server is a Bolt protocol server that accepts Neo4j driver connections.
type Server struct {
	listener  net.Listener
	executor  QueryExecutor
	addr      string
	tlsConfig *tls.Config
	authToken string // shared secret; empty = no auth
	running   atomic.Bool
	wg        sync.WaitGroup
}

// NewServer creates a new Bolt server.
func NewServer(addr string, executor QueryExecutor) *Server {
	return &Server{
		addr:     addr,
		executor: executor,
	}
}

// SetTLS configures TLS for the Bolt listener.
func (s *Server) SetTLS(cfg *tls.Config) {
	s.tlsConfig = cfg
}

// SetAuth configures token authentication for the Bolt server.
func (s *Server) SetAuth(token string) {
	s.authToken = token
}

// Start begins accepting connections.
func (s *Server) Start() error {
	var ln net.Listener
	var err error
	if s.tlsConfig != nil {
		ln, err = tls.Listen("tcp", s.addr, s.tlsConfig)
	} else {
		ln, err = net.Listen("tcp", s.addr)
	}
	if err != nil {
		return fmt.Errorf("bolt listen: %w", err)
	}
	s.listener = ln
	s.running.Store(true)

	s.wg.Add(1)
	go s.acceptLoop()

	slog.Info("bolt server listening", "addr", s.addr)
	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	s.running.Store(false)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
}

// Addr returns the listener address (useful for tests with :0).
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for s.running.Load() {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running.Load() {
				slog.Warn("bolt accept error", "err", err)
			}
			return
		}
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	c := &connection{
		conn:      conn,
		executor:  s.executor,
		authToken: s.authToken,
		state:     stateNegotiation,
	}

	if err := c.negotiate(); err != nil {
		slog.Debug("bolt handshake failed", "remote", conn.RemoteAddr(), "err", err)
		return
	}

	slog.Debug("bolt client connected", "remote", conn.RemoteAddr())

	for c.state != stateClosed {
		if err := c.processMessage(); err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "closed") {
				slog.Debug("bolt connection error", "remote", conn.RemoteAddr(), "err", err)
			}
			return
		}
	}
}

type connState int

const (
	stateNegotiation connState = iota
	stateAuthentication
	stateReady
	stateStreaming
	stateFailed
	stateClosed
)

type connection struct {
	conn      net.Conn
	executor  QueryExecutor
	authToken string
	state     connState

	// Current result set for streaming via PULL.
	columns []string
	rows    []map[string]any
	cursor  int
	qid     int64
	nextQID int64

	// Bookmark tracking for causal consistency.
	lastBookmark string
	bookmarkSeq  int64
}

func (c *connection) negotiate() error {
	// Read 4-byte magic preamble.
	magic := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, magic); err != nil {
		return fmt.Errorf("read magic: %w", err)
	}
	if !bytes.Equal(magic, boltMagic) {
		return fmt.Errorf("invalid magic: %x", magic)
	}

	// Read 4 version proposals (each 4 bytes).
	versions := make([]byte, 16)
	if _, err := io.ReadFull(c.conn, versions); err != nil {
		return fmt.Errorf("read versions: %w", err)
	}

	// Find the best version we support (look for 4.x).
	agreed := [4]byte{0, 0, 0, 0} // 0 = no agreement
	for i := 0; i < 4; i++ {
		v := versions[i*4 : i*4+4]
		major := v[3]
		minor := v[2]
		// range: v[1] is the minor range
		if major == 4 && minor >= 2 {
			agreed = [4]byte{0, 0, minor, major}
			break
		}
		// Also accept non-range proposals.
		if major == 4 {
			agreed = [4]byte{0, 0, minor, major}
			break
		}
	}

	// Respond with agreed version.
	if _, err := c.conn.Write(agreed[:]); err != nil {
		return fmt.Errorf("write version: %w", err)
	}

	if agreed == [4]byte{0, 0, 0, 0} {
		return fmt.Errorf("no supported version")
	}

	c.state = stateAuthentication
	return nil
}

func (c *connection) processMessage() error {
	data, err := ReadMessage(c.conn)
	if err != nil {
		return err
	}

	unpacker := NewUnpacker(bytes.NewReader(data))
	val, err := unpacker.Unpack()
	if err != nil {
		return fmt.Errorf("unpack message: %w", err)
	}

	msg, ok := val.(*BoltStruct)
	if !ok {
		return fmt.Errorf("expected struct, got %T", val)
	}

	switch msg.Tag {
	case msgHELLO:
		return c.handleHello(msg)
	case msgLOGON:
		return c.handleLogon(msg)
	case msgRUN:
		return c.handleRun(msg)
	case msgPULL:
		return c.handlePull(msg)
	case msgDISCARD:
		return c.handleDiscard(msg)
	case msgBEGIN:
		return c.handleBegin(msg)
	case msgCOMMIT:
		return c.handleCommit(msg)
	case msgROLLBACK:
		return c.handleRollback(msg)
	case msgRESET:
		return c.handleReset(msg)
	case msgGOODBYE:
		c.state = stateClosed
		return nil
	case msgROUTE:
		return c.handleRoute(msg)
	case msgLOGOFF:
		return c.handleLogoff(msg)
	case msgTELEMETRY:
		return c.handleTelemetry(msg)
	default:
		return c.sendFailure("Neo.ClientError.Request.Invalid",
			fmt.Sprintf("unknown message tag 0x%02X", msg.Tag))
	}
}

func (c *connection) handleHello(msg *BoltStruct) error {
	// HELLO {user_agent, scheme, principal, credentials, ...}
	if c.authToken != "" {
		authenticated := false
		if len(msg.Fields) >= 1 {
			if extra, ok := msg.Fields[0].(map[string]any); ok {
				if creds, ok := extra["credentials"].(string); ok {
					authenticated = subtle.ConstantTimeCompare([]byte(c.authToken), []byte(creds)) == 1
				}
			}
		}
		if !authenticated {
			return c.sendFailure("Neo.ClientError.Security.Unauthorized", "invalid credentials")
		}
	}
	c.state = stateReady
	return c.sendSuccess(map[string]any{
		"server":        "Loveliness/1.0.0",
		"connection_id": "bolt-1",
		"hints":         map[string]any{},
	})
}

func (c *connection) handleLogon(msg *BoltStruct) error {
	// LOGON for Bolt 5.x auth separation — just accept.
	return c.sendSuccess(nil)
}

func (c *connection) handleRun(msg *BoltStruct) error {
	if c.state == stateFailed {
		return c.sendIgnored()
	}

	if len(msg.Fields) < 1 {
		return c.sendFailure("Neo.ClientError.Request.Invalid", "RUN requires at least 1 field")
	}

	cypher, ok := msg.Fields[0].(string)
	if !ok {
		return c.sendFailure("Neo.ClientError.Request.Invalid", "RUN cypher must be a string")
	}

	// Extract params if provided.
	var params map[string]any
	if len(msg.Fields) >= 2 {
		if m, ok := msg.Fields[1].(map[string]any); ok {
			params = m
		}
	}

	// Accept extra map (field 2) with db, bookmarks, tx_timeout, tx_metadata, mode.
	// Single-database: we ignore db selection but accept it for compatibility.
	if len(msg.Fields) >= 3 {
		if extra, ok := msg.Fields[2].(map[string]any); ok {
			if bm, ok := extra["bookmarks"]; ok {
				if bmList, ok := bm.([]any); ok && len(bmList) > 0 {
					if s, ok := bmList[len(bmList)-1].(string); ok {
						c.lastBookmark = s
					}
				}
			}
			_ = extra["db"]
		}
	}

	// Interpolate parameters into Cypher (LadybugDB doesn't support $params natively).
	resolved := interpolateParams(cypher, params)

	columns, rows, err := c.executor.Query(resolved, nil)
	if err != nil {
		c.state = stateFailed
		return c.sendFailure("Neo.DatabaseError.Statement.ExecutionFailed", err.Error())
	}

	c.columns = columns
	c.rows = rows
	c.cursor = 0
	c.qid = c.nextQID
	c.nextQID++
	c.state = stateStreaming

	meta := map[string]any{
		"fields": toAnySlice(columns),
		"qid":    c.qid,
	}
	return c.sendSuccess(meta)
}

func (c *connection) handlePull(msg *BoltStruct) error {
	if c.state == stateFailed {
		return c.sendIgnored()
	}
	if c.state != stateStreaming {
		return c.sendFailure("Neo.ClientError.Request.Invalid", "no result to pull from")
	}

	// Parse pull size from extra map.
	pullN := -1 // default: pull all
	if len(msg.Fields) >= 1 {
		if extra, ok := msg.Fields[0].(map[string]any); ok {
			if n, ok := extra["n"]; ok {
				if nInt, ok := n.(int64); ok {
					pullN = int(nInt)
				}
			}
		}
	}

	p := NewPacker()
	sent := 0
	for c.cursor < len(c.rows) {
		if pullN >= 0 && sent >= pullN {
			break
		}
		row := c.rows[c.cursor]
		fields := RowToFields(c.columns, row)
		p.Reset()
		packRecord(p, fields)
		if err := WriteMessage(c.conn, p.Bytes()); err != nil {
			return err
		}
		c.cursor++
		sent++
	}

	hasMore := c.cursor < len(c.rows)
	meta := map[string]any{}
	if hasMore {
		meta["has_more"] = true
		meta["qid"] = c.qid
	} else {
		meta["has_more"] = false
		c.bookmarkSeq++
		meta["bookmark"] = fmt.Sprintf("loveliness:v1:tx%d", c.bookmarkSeq)
		c.state = stateReady
		c.rows = nil
		c.columns = nil
	}

	p.Reset()
	packSuccess(p, meta)
	return WriteMessage(c.conn, p.Bytes())
}

func (c *connection) handleDiscard(msg *BoltStruct) error {
	if c.state == stateFailed {
		return c.sendIgnored()
	}
	c.rows = nil
	c.columns = nil
	c.cursor = 0
	c.state = stateReady
	return c.sendSuccess(nil)
}

func (c *connection) handleBegin(msg *BoltStruct) error {
	if c.state == stateFailed {
		return c.sendIgnored()
	}
	// Accept extra map with db, bookmarks, tx_timeout, tx_metadata, mode, etc.
	// We don't enforce any of these but we accept them for compatibility.
	if len(msg.Fields) >= 1 {
		if extra, ok := msg.Fields[0].(map[string]any); ok {
			// Accept bookmarks — track last received for causal consistency.
			if bm, ok := extra["bookmarks"]; ok {
				if bmList, ok := bm.([]any); ok && len(bmList) > 0 {
					if s, ok := bmList[len(bmList)-1].(string); ok {
						c.lastBookmark = s
					}
				}
			}
			// Accept db param — single-database, so we ignore the value.
			_ = extra["db"]
		}
	}
	return c.sendSuccess(nil)
}

func (c *connection) handleCommit(msg *BoltStruct) error {
	if c.state == stateFailed {
		return c.sendIgnored()
	}
	return c.sendSuccess(nil)
}

func (c *connection) handleRollback(msg *BoltStruct) error {
	if c.state == stateFailed {
		return c.sendIgnored()
	}
	c.state = stateReady
	return c.sendSuccess(nil)
}

func (c *connection) handleReset(msg *BoltStruct) error {
	c.state = stateReady
	c.rows = nil
	c.columns = nil
	c.cursor = 0
	return c.sendSuccess(nil)
}

func (c *connection) handleRoute(msg *BoltStruct) error {
	// ROUTE message (Bolt 4.3+) for multi-database routing.
	// Return a single server entry pointing at ourselves.
	addr := c.conn.LocalAddr().String()
	return c.sendSuccess(map[string]any{
		"rt": map[string]any{
			"ttl": int64(300),
			"servers": []any{
				map[string]any{
					"role":      "WRITE",
					"addresses": []any{addr},
				},
				map[string]any{
					"role":      "READ",
					"addresses": []any{addr},
				},
				map[string]any{
					"role":      "ROUTE",
					"addresses": []any{addr},
				},
			},
		},
	})
}

func (c *connection) handleLogoff(msg *BoltStruct) error {
	// LOGOFF (Bolt 5.1+) — de-authenticate the connection.
	// We accept it and return to authentication state for re-auth.
	c.state = stateAuthentication
	return c.sendSuccess(nil)
}

func (c *connection) handleTelemetry(msg *BoltStruct) error {
	// TELEMETRY (Bolt 5.4+) — driver usage metrics.
	// Accept and acknowledge; we don't collect telemetry.
	return c.sendSuccess(nil)
}

func (c *connection) sendSuccess(meta map[string]any) error {
	p := NewPacker()
	packSuccess(p, meta)
	return WriteMessage(c.conn, p.Bytes())
}

func (c *connection) sendFailure(code, message string) error {
	p := NewPacker()
	packFailure(p, code, message)
	c.state = stateFailed
	return WriteMessage(c.conn, p.Bytes())
}

func (c *connection) sendIgnored() error {
	p := NewPacker()
	packIgnored(p)
	return WriteMessage(c.conn, p.Bytes())
}

// interpolateParams replaces $param references in Cypher with literal values.
// LadybugDB (Kuzu) doesn't support parameterized queries the same way Neo4j does.
func interpolateParams(cypher string, params map[string]any) string {
	if len(params) == 0 {
		return cypher
	}
	result := cypher
	for key, val := range params {
		placeholder := "$" + key
		literal := paramToLiteral(val)
		result = strings.ReplaceAll(result, placeholder, literal)
	}
	return result
}

func paramToLiteral(v any) string {
	switch val := v.(type) {
	case string:
		escaped := strings.ReplaceAll(val, "'", "\\'")
		return "'" + escaped + "'"
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case nil:
		return "NULL"
	case []any:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = paramToLiteral(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		parts := make([]string, 0, len(val))
		for k, v := range val {
			parts = append(parts, k+": "+paramToLiteral(v))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprintf("'%v'", val)
	}
}

func toAnySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// WriteVersionNeg writes a version negotiation response directly (for testing).
func WriteVersionNeg(w io.Writer, version [4]byte) error {
	_, err := w.Write(version[:])
	return err
}

// ReadVersionNeg reads a version negotiation response.
func ReadVersionNeg(r io.Reader) ([4]byte, error) {
	var v [4]byte
	_, err := io.ReadFull(r, v[:])
	return v, err
}
