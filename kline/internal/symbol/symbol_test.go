package symbol

import "testing"

func TestToMEXC(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "btc usdt", in: "BTCUSDT", want: "BTC_USDT"},
		{name: "eth usdc", in: "ETHUSDC", want: "ETH_USDC"},
		{name: "xrp btc", in: "XRPBTC", want: "XRP_BTC"},
		{name: "usdc usdt precedence", in: "USDCUSDT", want: "USDC_USDT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToMEXC(tt.in)
			if got != tt.want {
				t.Fatalf("ToMEXC(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestToFile(t *testing.T) {
	in := "BTCUSDT"
	if got := ToFile(in); got != in {
		t.Fatalf("ToFile(%q) = %q, want %q", in, got, in)
	}
}
