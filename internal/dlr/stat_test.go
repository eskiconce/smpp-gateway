package dlr

import "testing"

func TestMapStat(t *testing.T) {
	cases := map[string]string{
		"DELIVRD":  "delivered",
		"EXPIRED":  "expired",
		"UNDELIV":  "undeliv",
		"REJECTED": "rejected",
		"ENROUTE":  "",
		"":         "",
	}
	for in, want := range cases {
		if got := MapStat(in); got != want {
			t.Fatalf("MapStat(%q)=%q want %q", in, got, want)
		}
	}
}
