package mcp

import (
	"context"
	"errors"
	"testing"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode int
		wantMsg  string
	}{
		{
			name:     "nil",
			err:      nil,
			wantCode: 0,
		},
		{
			name:     "context deadline",
			err:      context.DeadlineExceeded,
			wantCode: codeTimeout,
			wantMsg:  "query exceeded the configured timeout",
		},
		{
			name:     "400 cypher parse",
			err:      &HTTPError{Status: 400, Code: "CYPHER_PARSE_ERROR", Msg: "syntax"},
			wantCode: codeInvalidParams,
			wantMsg:  "cypher parse error",
		},
		{
			name:     "409 broadcast partial",
			err:      &HTTPError{Status: 409, Code: "BROADCAST_PARTIAL", Msg: "x"},
			wantCode: codeServerError,
			wantMsg:  "schema propagation",
		},
		{
			name:     "503 shard unavailable",
			err:      &HTTPError{Status: 503, Code: "SHARD_UNAVAILABLE", Msg: "shard 2 down"},
			wantCode: codeServerError,
			wantMsg:  "shard unavailable",
		},
		{
			name:     "504 gateway timeout",
			err:      &HTTPError{Status: 504, Code: "QUERY_TIMEOUT", Msg: "deadline"},
			wantCode: codeTimeout,
			wantMsg:  "query exceeded",
		},
		{
			name:     "401 unauthorized",
			err:      &HTTPError{Status: 401, Code: "UNAUTHORIZED", Msg: "bad token"},
			wantCode: codeServerError,
			wantMsg:  "authentication failed",
		},
		{
			name:     "generic 500",
			err:      &HTTPError{Status: 500, Msg: "boom"},
			wantCode: codeServerError,
			wantMsg:  "loveliness 500",
		},
		{
			name:     "plain error",
			err:      errors.New("something broke"),
			wantCode: codeServerError,
			wantMsg:  "something broke",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, msg := classify(tc.err)
			if code != tc.wantCode {
				t.Errorf("code = %d, want %d", code, tc.wantCode)
			}
			if tc.wantMsg != "" && !stringContains(msg, tc.wantMsg) {
				t.Errorf("msg = %q, want to contain %q", msg, tc.wantMsg)
			}
		})
	}
}
