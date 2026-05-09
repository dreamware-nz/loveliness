package router

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// Retry policy for remote shard RPCs.
//
// The router treats only transient transport-layer faults as
// retryable: a peer that's restarting, a TCP frame interrupted
// mid-flight, a connection refused while a node finishes booting.
// Anything that looks like a protocol error or a query error is
// surfaced immediately — retrying those would just multiply the
// blast radius of a deterministic failure.
//
// The cap is small on purpose: 2 retries with jittered backoff is
// enough to ride out a single restart or a brief network blip and
// short enough that a sustained outage still surfaces in the
// per-shard error within the scatter timeout. A user that needs
// stronger SLAs can raise it via SetRemoteRetries.

// defaultRemoteRetries is the spec-stated cap from #6.
const defaultRemoteRetries = 2

// defaultRemoteRetryBackoff is the base delay between retries. The
// real delay is jittered to avoid thundering-herd patterns when many
// shards on the same peer all retry at once.
const defaultRemoteRetryBackoff = 25 * time.Millisecond

// isRetryableRemoteError reports whether err describes a transient
// transport fault that's worth retrying. The check is structural —
// errors.Is / errors.As / type assertions on the unwrap chain — so
// it survives the transport layer wrapping errors with fmt.Errorf
// %w. We deliberately do not match on substrings: error messages
// drift between Go versions, but the underlying types do not.
func isRetryableRemoteError(err error) bool {
	if err == nil {
		return false
	}
	// Context cancel is a caller signal, not a transport fault — never
	// retry it.
	if errors.Is(err, context.Canceled) {
		return false
	}
	// Deadline exceeded on a per-call basis is a hint that the peer is
	// slow; one retry can ride out a brief stall.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// Mid-frame EOFs and unexpected EOFs are the canonical
	// "connection died while we were reading" signals.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	// Timeout on a net.Op (dial, read, write).
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	// "Connection refused" / "connection reset" / "broken pipe" all
	// surface as syscall errors wrapped in *net.OpError. We don't
	// reach into the syscall package because the constants are
	// platform-specific; the message form is stable enough across
	// Linux/macOS/BSD that a single substring check is acceptable
	// for this narrow case (only matched after we've already
	// confirmed the error is a net.OpError, so we won't false-
	// positive on unrelated strings).
	var op *net.OpError
	if errors.As(err, &op) {
		msg := op.Err.Error()
		switch {
		case strings.Contains(msg, "connection refused"),
			strings.Contains(msg, "connection reset"),
			strings.Contains(msg, "broken pipe"):
			return true
		}
	}
	return false
}

// retryRemoteCall invokes fn up to maxRetries+1 times, sleeping a
// jittered backoff between attempts when fn returns a retryable
// error. Non-retryable errors short-circuit immediately. The result
// of the final attempt — whether success or failure — is returned
// to the caller.
//
// fn should not internally retry; this is the only retry layer for
// remote calls. Layered retries silently amplify load on a flapping
// peer.
func retryRemoteCall(
	ctx context.Context,
	maxRetries int,
	baseBackoff time.Duration,
	fn func() (*shard.QueryResponse, error),
) (*shard.QueryResponse, error) {
	if maxRetries < 0 {
		maxRetries = 0
	}
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		resp, err := fn()
		if err == nil {
			return resp, nil
		}
		if !isRetryableRemoteError(err) {
			return nil, err
		}
		lastErr = err
		if attempt == maxRetries {
			break // out of retries; surface the last error below
		}
		// Jittered backoff: base * 2^attempt with ±50% jitter. Caps
		// the worst case for maxRetries=2 at ~1.5 * base + 3 * base
		// = 4.5 * base, which at 25ms is well under any sane scatter
		// timeout but long enough to ride out a quick restart.
		delay := backoffFor(baseBackoff, attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// backoffFor returns base * 2^attempt with ±50% jitter. Attempt is
// 0-indexed; attempt=0 yields base ± 50%.
func backoffFor(base time.Duration, attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	mult := time.Duration(1) << uint(attempt)
	scaled := base * mult
	// ±50% jitter via rand. The package-level rand is fine — we just
	// want enough spread to break thundering-herd patterns.
	jitter := time.Duration(rand.Int63n(int64(scaled)+1)) - scaled/2
	return scaled + jitter
}
