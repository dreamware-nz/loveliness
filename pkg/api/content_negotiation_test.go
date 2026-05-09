package api

import "testing"

func TestNegotiate_EmptyAcceptIsJSON(t *testing.T) {
	got, bad := negotiateResponse("")
	if bad {
		t.Errorf("empty Accept must not be 406")
	}
	if got != contentTypeJSON {
		t.Errorf("expected JSON, got %q", got)
	}
}

func TestNegotiate_WildcardIsJSON(t *testing.T) {
	got, bad := negotiateResponse("*/*")
	if bad || got != contentTypeJSON {
		t.Errorf("*/* must resolve to JSON, got %q (bad=%v)", got, bad)
	}
}

func TestNegotiate_ApplicationWildcardIsJSON(t *testing.T) {
	got, bad := negotiateResponse("application/*")
	if bad || got != contentTypeJSON {
		t.Errorf("application/* must resolve to JSON (preserves status quo), got %q (bad=%v)", got, bad)
	}
}

func TestNegotiate_ExplicitArrowFile(t *testing.T) {
	got, bad := negotiateResponse(contentTypeArrowFile)
	if bad || got != contentTypeArrowFile {
		t.Errorf("explicit arrow.file must win, got %q (bad=%v)", got, bad)
	}
}

func TestNegotiate_ExplicitArrowStream(t *testing.T) {
	got, bad := negotiateResponse(contentTypeArrowStream)
	if bad || got != contentTypeArrowStream {
		t.Errorf("explicit arrow.stream must win, got %q (bad=%v)", got, bad)
	}
}

func TestNegotiate_QualityValues_ArrowWins(t *testing.T) {
	got, _ := negotiateResponse("application/vnd.apache.arrow.stream;q=0.9, application/json;q=0.5")
	if got != contentTypeArrowStream {
		t.Errorf("higher q for arrow.stream must win, got %q", got)
	}
}

func TestNegotiate_QualityValues_JSONWins(t *testing.T) {
	got, _ := negotiateResponse("application/vnd.apache.arrow.stream;q=0.3, application/json;q=0.9")
	if got != contentTypeJSON {
		t.Errorf("higher q for json must win, got %q", got)
	}
}

func TestNegotiate_QZeroSkipsType(t *testing.T) {
	got, bad := negotiateResponse("application/json;q=0, application/vnd.apache.arrow.file")
	if bad {
		t.Errorf("must not be 406 when arrow.file is acceptable")
	}
	if got != contentTypeArrowFile {
		t.Errorf("q=0 must remove json from candidates, got %q", got)
	}
}

func TestNegotiate_UnknownConcreteTypesAre406(t *testing.T) {
	got, bad := negotiateResponse("application/xml, text/plain")
	if !bad {
		t.Errorf("must report 406 when no offer matches, got %q", got)
	}
}

func TestNegotiate_MixedWildcardKeepsJSON(t *testing.T) {
	// */* alongside an unsupported type — wildcard rescues us into JSON.
	got, bad := negotiateResponse("application/xml, */*")
	if bad {
		t.Errorf("*/* must save us from 406")
	}
	if got != contentTypeJSON {
		t.Errorf("expected JSON via wildcard, got %q", got)
	}
}

func TestNegotiate_TieBreakByOrder(t *testing.T) {
	// Both arrow.file and json have q=1; first listed should win.
	got, _ := negotiateResponse("application/vnd.apache.arrow.file, application/json")
	if got != contentTypeArrowFile {
		t.Errorf("first-listed equal-q offer must win, got %q", got)
	}
}

func TestNegotiate_CaseInsensitiveMediaType(t *testing.T) {
	got, _ := negotiateResponse("APPLICATION/VND.APACHE.ARROW.FILE")
	if got != contentTypeArrowFile {
		t.Errorf("media type match must be case-insensitive, got %q", got)
	}
}
