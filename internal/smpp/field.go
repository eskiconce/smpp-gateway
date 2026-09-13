// internal/smpp/field.go
package smpp

import (
	"encoding/binary"
	"errors"
)

func GetU32(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func PutU32(b []byte, v uint32) { binary.BigEndian.PutUint32(b, v) }

func PutU16(b []byte, v uint16) { binary.BigEndian.PutUint16(b, v) }

type Reader struct {
	b   []byte
	off int
}

func (r *Reader) U8() (uint8, error) {
	if r.off+1 > len(r.b) {
		return 0, errors.New("smpp: short read u8")
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}

func (r *Reader) CString() (string, error) {
	start := r.off
	for r.off < len(r.b) && r.b[r.off] != 0 {
		r.off++
	}
	if r.off >= len(r.b) {
		return "", errors.New("smpp: cstring sin terminar")
	}
	s := string(r.b[start:r.off])
	r.off++ // salta el \0
	return s, nil
}

func (r *Reader) Bytes(n int) ([]byte, error) {
	if r.off+n > len(r.b) {
		return nil, errors.New("smpp: short read bytes")
	}
	v := r.b[r.off : r.off+n]
	r.off += n
	return v, nil
}

func (r *Reader) Remaining() []byte { return r.b[r.off:] }
