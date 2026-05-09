package api

import (
	"strconv"
	"strings"
)

// Response content types the /cypher endpoint can produce.
const (
	contentTypeJSON        = "application/json"
	contentTypeArrowStream = "application/vnd.apache.arrow.stream"
	contentTypeArrowFile   = "application/vnd.apache.arrow.file"
)

// negotiateResponse picks a response content type for the /cypher
// endpoint from the request's Accept header. Returns the chosen
// concrete media type and a bool indicating whether the client
// explicitly asked for something we cannot serve (in which case the
// caller should return 406 Not Acceptable).
//
// JSON stays the canonical default: an empty Accept, "*/*", or a
// header that doesn't list any of our supported types resolves to
// JSON. Quality values are honored — the highest q-weighted
// supported type wins, ties broken by header order.
//
// We only flag "unacceptable" when every offered type is concrete
// (no wildcards) and none of them is supported by the server.
func negotiateResponse(accept string) (chosen string, unacceptable bool) {
	accept = strings.TrimSpace(accept)
	if accept == "" {
		return contentTypeJSON, false
	}

	type offer struct {
		mediaType string
		q         float64
		index     int
	}

	var offers []offer
	for i, raw := range strings.Split(accept, ",") {
		mt, q, ok := parseAcceptOffer(raw)
		if !ok {
			continue
		}
		offers = append(offers, offer{mediaType: mt, q: q, index: i})
	}
	if len(offers) == 0 {
		return contentTypeJSON, false
	}

	bestSupported := offer{q: -1}
	hasWildcard := false
	hasJSONWildcard := false
	for _, o := range offers {
		if o.mediaType == "*/*" {
			hasWildcard = true
			continue
		}
		if o.mediaType == "application/*" {
			hasJSONWildcard = true
			continue
		}
		if !isSupportedType(o.mediaType) {
			continue
		}
		if o.q > bestSupported.q || (o.q == bestSupported.q && o.index < bestSupported.index) {
			bestSupported = o
		}
	}

	if bestSupported.q > 0 {
		return bestSupported.mediaType, false
	}
	if hasWildcard || hasJSONWildcard {
		return contentTypeJSON, false
	}
	return "", true
}

// parseAcceptOffer extracts the media type and q value from a single
// Accept header offer. Returns (mediaType, q, ok). q defaults to 1.0
// per RFC 7231; q=0 means "not acceptable" and is treated as ok=false.
func parseAcceptOffer(raw string) (string, float64, bool) {
	parts := strings.Split(strings.TrimSpace(raw), ";")
	if len(parts) == 0 {
		return "", 0, false
	}
	mt := strings.ToLower(strings.TrimSpace(parts[0]))
	if mt == "" {
		return "", 0, false
	}
	q := 1.0
	for _, p := range parts[1:] {
		p = strings.TrimSpace(p)
		if !strings.HasPrefix(p, "q=") {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimPrefix(p, "q="), 64)
		if err != nil {
			continue
		}
		q = v
	}
	if q <= 0 {
		return "", 0, false
	}
	return mt, q, true
}

func isSupportedType(mt string) bool {
	switch mt {
	case contentTypeJSON, contentTypeArrowStream, contentTypeArrowFile:
		return true
	default:
		return false
	}
}
