package smpp

const (
	maxPerPart       = 160
	maxPerPartConcat = 153
)

// SplitText divide texto largo en segmentos. Simplifica: codifica como GSM-7
// (charset simple, 1 byte/char). >1 segmento aplica UDH concat de 6 bytes
// (SAR: 3 bytes IE). Unicode se introduce en M2/M3.
func SplitText(text string) ([]string, int, error) {
	runes := []rune(text)
	if len(runes) <= maxPerPart {
		return []string{text}, 1, nil
	}
	var parts []string
	for len(runes) > 0 {
		n := len(runes)
		if n > maxPerPartConcat {
			n = maxPerPartConcat
		}
		parts = append(parts, string(runes[:n]))
		runes = runes[n:]
	}
	return parts, len(parts), nil
}

// ConcatUDH construye el UDH SAR para un segmento (1-based).
func ConcatUDH(ref byte, total, part byte) string {
	return "\x05\x00\x03" + string([]byte{ref, total, part})
}
