// Package strutil contains UTF-8 aware string helpers.
package strutil

import (
	"unicode"
	"unicode/utf8"
)

// CapitalizeFirst upper-cases the first rune in message.
// Invalid UTF-8 is left unchanged.
func CapitalizeFirst(message string) string {
	r, size := utf8.DecodeRuneInString(message)
	if r == utf8.RuneError {
		return message
	}
	return string(unicode.ToUpper(r)) + message[size:]
}
