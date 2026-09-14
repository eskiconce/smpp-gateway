package dlr

var statMap = map[string]string{
	"DELIVRD":  "delivered",
	"EXPIRED":  "expired",
	"UNDELIV":  "undeliv",
	"REJECTED": "rejected",
}

func MapStat(stat string) string { return statMap[stat] }
