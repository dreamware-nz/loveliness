package bolt

// Bolt v4.x message tags.
const (
	// Client messages.
	msgHELLO   byte = 0x01
	msgGOODBYE byte = 0x02
	msgLOGON   byte = 0x6A
	msgRUN     byte = 0x10
	msgPULL    byte = 0x3F
	msgDISCARD byte = 0x2F
	msgBEGIN   byte = 0x11
	msgCOMMIT  byte = 0x12
	msgROLLBACK byte = 0x13
	msgRESET    byte = 0x0F
	msgROUTE    byte = 0x66
	msgLOGOFF   byte = 0x6B
	msgTELEMETRY byte = 0x54

	// Server messages.
	msgSUCCESS byte = 0x70
	msgRECORD  byte = 0x71
	msgIGNORED byte = 0x7E
	msgFAILURE byte = 0x7F
)

// packSuccess packs a SUCCESS message with metadata.
func packSuccess(p *Packer, metadata map[string]any) {
	p.PackStructHeader(1, msgSUCCESS)
	if metadata == nil {
		p.PackMapHeader(0)
	} else {
		p.PackMapHeader(len(metadata))
		for k, v := range metadata {
			p.PackString(k)
			p.PackValue(v)
		}
	}
}

// packFailure packs a FAILURE message.
func packFailure(p *Packer, code, message string) {
	p.PackStructHeader(1, msgFAILURE)
	p.PackMapHeader(2)
	p.PackString("code")
	p.PackString(code)
	p.PackString("message")
	p.PackString(message)
}

// packIgnored packs an IGNORED message.
func packIgnored(p *Packer) {
	p.PackStructHeader(0, msgIGNORED)
}

// packRecord packs a RECORD message with field values.
func packRecord(p *Packer, fields []any) {
	p.PackStructHeader(1, msgRECORD)
	p.PackListHeader(len(fields))
	for _, f := range fields {
		p.PackValue(f)
	}
}
