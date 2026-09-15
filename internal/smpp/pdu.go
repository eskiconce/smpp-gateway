package smpp

import (
	"errors"
	"strings"
)

type BindFields struct {
	SystemID  string
	Password  string
	SysType   string
	Ton, Npi  uint8
	AddrRange string
}

type SmFields struct {
	ServiceType                        string
	SrcTon, SrcNpi                     uint8
	SourceAddr                         string
	DstTon, DstNpi                     uint8
	DestAddr                           string
	ESMClass, ProtocolID, PriorityFlag uint8
	RegisteredDelivery                 uint8
	DataCoding                         uint8
	ShortMessage                       string
	MessagePayload                     []byte
}

const (
	TLVMessagePayload uint16 = 0x0424
	TLVUserMessageRef uint16 = 0x0204
)

func NewBindTransceiver(seq uint32, systemID, password, sysType string, ton, npi uint8, addrRange string) *PDU {
	s := systemID + "\x00" + password + "\x00" + sysType + "\x00"
	s += string([]byte{ton, npi}) + addrRange + "\x00"
	return &PDU{Header: Head(seq, BindTransceiver), Body: []byte(s)}
}

func ParseBindTransceiver(b []byte) (BindFields, error) {
	r := &Reader{b: b}
	var f BindFields
	var err error
	if f.SystemID, err = r.CString(); err != nil {
		return f, err
	}
	if f.Password, err = r.CString(); err != nil {
		return f, err
	}
	if f.SysType, err = r.CString(); err != nil {
		return f, err
	}
	if f.Ton, err = r.U8(); err != nil {
		return f, err
	}
	if f.Npi, err = r.U8(); err != nil {
		return f, err
	}
	if f.AddrRange, err = r.CString(); err != nil {
		return f, err
	}
	return f, nil
}

func NewBindTransceiverResp(seq uint32, status CommandStatus, sysID string) *PDU {
	h := Head(seq, BindTransceiverResp)
	h.Status = status
	return &PDU{Header: h, Body: []byte(sysID + "\x00")}
}

func NewSubmitSM(seq uint32, source, dest, text string, dataCoding, regDelivery uint8) (*PDU, error) {
	h := Head(seq, SubmitSM)
	body := submitBody(source, dest, text, dataCoding, regDelivery)
	var tlvs map[uint16][]byte
	if len(text) > 254 {
		tlvs = map[uint16][]byte{TLVMessagePayload: []byte(text)}
	}
	return &PDU{Header: h, Body: body, TLVs: tlvs}, nil
}

func submitBody(source, dest, text string, dataCoding, regDelivery uint8) []byte {
	b := make([]byte, 0, 64+len(text))
	b = append(b, 0x00) // service_type
	b = append(b, 0, 0) // src ton, npi
	b = append(b, source...)
	b = append(b, 0x00)
	b = append(b, 1, 1) // dst ton=1, npi=1
	b = append(b, dest...)
	b = append(b, 0x00)
	b = append(b, 0x00) // esm_class
	b = append(b, 0x00) // protocol_id
	b = append(b, 0x00) // priority
	b = append(b, 0x00) // schedule_delivery_time
	b = append(b, 0x00) // validity_period
	b = append(b, regDelivery)
	b = append(b, 0x00) // replace_if_present
	b = append(b, dataCoding)
	b = append(b, 0x00) // sm_default_msg_id
	if len(text) <= 254 {
		b = append(b, uint8(len(text)))
		b = append(b, text...)
	} else {
		b = append(b, 0x00) // sm_length 0, mensaje por TLV
	}
	return b
}

func ParseSubmitSM(b []byte) (SmFields, error) {
	return parseSm(b)
}

func parseSm(b []byte) (SmFields, error) {
	r := &Reader{b: b, off: 0}
	var f SmFields
	var err error
	if f.ServiceType, err = r.CString(); err != nil {
		return f, err
	}
	if f.SrcTon, err = r.U8(); err != nil {
		return f, err
	}
	if f.SrcNpi, err = r.U8(); err != nil {
		return f, err
	}
	if f.SourceAddr, err = r.CString(); err != nil {
		return f, err
	}
	if f.DstTon, err = r.U8(); err != nil {
		return f, err
	}
	if f.DstNpi, err = r.U8(); err != nil {
		return f, err
	}
	if f.DestAddr, err = r.CString(); err != nil {
		return f, err
	}
	if f.ESMClass, err = r.U8(); err != nil {
		return f, err
	}
	if f.ProtocolID, err = r.U8(); err != nil {
		return f, err
	}
	if f.PriorityFlag, err = r.U8(); err != nil {
		return f, err
	}
	if err = skipCStrings(r, 2); err != nil { // schedule_delivery_time, validity_period
		return f, err
	}
	if f.RegisteredDelivery, err = r.U8(); err != nil {
		return f, err
	}
	if _, err = r.U8(); err != nil { // replace_if_present
		return f, err
	}
	if f.DataCoding, err = r.U8(); err != nil {
		return f, err
	}
	if _, err = r.U8(); err != nil { // sm_default_msg_id
		return f, err
	}
	smLen, err := r.U8()
	if err != nil {
		return f, err
	}
	if smLen > 0 {
		raw, err := r.Bytes(int(smLen))
		if err != nil {
			return f, err
		}
		f.ShortMessage = string(raw)
	}
	// TLVs restantes
	for _, t := range parseTLVs(r.Remaining()) {
		switch t.Tag {
		case TLVMessagePayload:
			f.MessagePayload = t.Value
		}
	}
	return f, nil
}

func skipCStrings(r *Reader, n int) error {
	for i := 0; i < n; i++ {
		if _, err := r.CString(); err != nil {
			return err
		}
	}
	return nil
}

type tlv struct {
	Tag   uint16
	Value []byte
}

func parseTLVs(b []byte) []tlv {
	out := make([]tlv, 0, 2)
	i := 0
	for i+4 <= len(b) {
		tag := GetU16(b[i:])
		n := int(GetU16(b[i+2:]))
		i += 4
		if i+n > len(b) {
			break
		}
		out = append(out, tlv{Tag: tag, Value: b[i : i+n]})
		i += n
	}
	return out
}

func NewSubmitSMResp(seq uint32, status CommandStatus, msgid string) *PDU {
	return &PDU{Header: Header{Length: HeaderLen + uint32(len(msgid)+1), ID: SubmitSMResp, Status: status, Seq: seq}, Body: []byte(msgid + "\x00")}
}

func ParseSubmitSMResp(b []byte) (string, error) {
	r := &Reader{b: b}
	s, err := r.CString()
	if err != nil {
		return "", err
	}
	return s, nil
}

func NewDeliverSM(seq uint32, src, dst, dlrText string) *PDU {
	b := submitBody(src, dst, dlrText, 0, 0) // deliver_sm usa la misma estructura de body
	return &PDU{Header: Head(seq, DeliverSM), Body: b}
}

func ParseDeliverSM(b []byte) (SmFields, error) { return parseSm(b) }

func NewEnquireLink(seq uint32) *PDU {
	return &PDU{Header: Head(seq, EnquireLink)}
}

func NewEnquireLinkResp(seq uint32) *PDU {
	return &PDU{Header: Head(seq, EnquireLinkResp)}
}

// ParseDLR extrae el campo stat del texto del DLR (estilo SMSC 3.4).
func ParseDLR(msg string) (string, error) {
	parts := splitBySpaceKey(msg)
	stat, ok := parts["stat:"]
	if !ok || stat == "" {
		return "", errors.New("dlr: sin campo stat")
	}
	return stat, nil
}

func splitBySpaceKey(s string) map[string]string {
	out := map[string]string{}
	for _, f := range strings.Fields(strings.TrimSpace(s)) {
		if i := strings.Index(f, ":"); i > 0 {
			out[f[:i+1]] = f[i+1:]
		}
	}
	return out
}
