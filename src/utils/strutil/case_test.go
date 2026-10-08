package strutil

import "testing"

func TestCapitalizeFirst(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "ascii", in: "service started", want: "Service started"},
		{name: "unicode", in: "árvízturo", want: "Árvízturo"},
		{name: "empty", in: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CapitalizeFirst(tt.in); got != tt.want {
				t.Fatalf("CapitalizeFirst(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
