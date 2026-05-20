package symbol

import "strings"

// ToMEXC converts a user symbol to the MEXC Futures wire format.
func ToMEXC(s string) string {
	s = strings.TrimSpace(s)
	suffixes := []string{"USDT", "USDC", "BTC"}

	for _, suffix := range suffixes {
		if strings.HasSuffix(s, suffix) {
			base := strings.TrimSuffix(s, suffix)
			if base == "" {
				return s
			}
			return base + "_" + suffix
		}
	}

	return s
}

// ToFile returns the symbol used for output filenames.
func ToFile(s string) string {
	return strings.TrimSpace(s)
}
