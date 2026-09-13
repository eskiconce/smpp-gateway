// internal/smpp/codec.go
package smpp

import (
	"fmt"
)

type CommandID uint32

const (
	GenericNack     CommandID = 0x80000000
	BindReceiver    CommandID = 0x00000001
	BindTransmitter CommandID = 0x00000002
	SubmitSM        CommandID = 0x00000004
	DeliverSM       CommandID = 0x00000005
	Unbind          CommandID = 0x00000006
	BindTransceiver CommandID = 0x00000009
	EnquireLink     CommandID = 0x00000015
	DataSM          CommandID = 0x00000103

	BindReceiverResp    CommandID = 0x80000001
	BindTransmitterResp CommandID = 0x80000002
	SubmitSMResp        CommandID = 0x80000004
	DeliverSMResp       CommandID = 0x80000005
	UnbindResp          CommandID = 0x80000006
	BindTransceiverResp CommandID = 0x80000009
	EnquireLinkResp     CommandID = 0x80000015
)

type CommandStatus uint32

const (
	ESME_ROK         CommandStatus = 0x00
	ESME_RINVCMDLEN  CommandStatus = 0x02
	ESME_RBINDFAIL   CommandStatus = 0x0B
	ESME_RINVPASWD   CommandStatus = 0x0C
	ESME_RINVSYSTID  CommandStatus = 0x0D
	ESME_RTHROTTLED  CommandStatus = 0x34
	ESME_RUNKNOWNERR CommandStatus = 0xFF
)

const HeaderLen = 16

type Header struct {
	Length uint32
	ID     CommandID
	Status CommandStatus
	Seq    uint32
}

type PDU struct {
	Header Header
	Body   []byte
	TLVs   map[uint16][]byte
}

func Head(seq uint32, id CommandID) Header {
	return Header{Length: HeaderLen, ID: id, Status: ESME_ROK, Seq: seq}
}

func Encode(p *PDU) []byte {
	total := HeaderLen + len(p.Body) + tlvsSize(p.TLVs)
	b := make([]byte, total)
	PutU32(b[0:], uint32(total))
	PutU32(b[4:], uint32(p.Header.ID))
	PutU32(b[8:], uint32(p.Header.Status))
	PutU32(b[12:], p.Header.Seq)
	copy(b[HeaderLen:], p.Body)
	off := HeaderLen + len(p.Body)
	for tag, val := range p.TLVs {
		PutU16(b[off:], tag)
		PutU16(b[off+2:], uint16(len(val)))
		copy(b[off+4:], val)
		off += 4 + len(val)
	}
	return b
}

func Decode(b []byte) (*PDU, error) {
	if len(b) < HeaderLen {
		return nil, fmt.Errorf("smpp: pdu corto: %d bytes", len(b))
	}
	length := GetU32(b[0:])
	if int(length) != len(b) {
		return nil, fmt.Errorf("smpp: longitud declara %d != recibida %d", length, len(b))
	}
	p := &PDU{
		Header: Header{
			Length: length,
			ID:     CommandID(GetU32(b[4:])),
			Status: CommandStatus(GetU32(b[8:])),
			Seq:    GetU32(b[12:]),
		},
		Body: b[HeaderLen:length],
	}
	return p, nil
}

func (p *PDU) SetTLV(tag uint16, val []byte) {
	if p.TLVs == nil {
		p.TLVs = map[uint16][]byte{}
	}
	p.TLVs[tag] = val
}

func tlvsSize(tlvs map[uint16][]byte) int {
	n := 0
	for _, v := range tlvs {
		n += 4 + len(v)
	}
	return n
}
