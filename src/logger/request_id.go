package logger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
)

type requestIDKey struct{}

// WithRequestID returns a context that carries id.
// The handler copies that id onto every record made with the context,
// and a worker can read the same value with RequestID.
func WithRequestID(ctx context.Context, id string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID returns the id stored by WithRequestID, or "" when ctx has none.
func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// ResolveRequestID keeps clientID when every character is safe.
// An empty value, or one with any other character, is replaced with an id
// the server generated. The worker carries that same id forward.
//
// Safe characters are ASCII letters, digits, '.', '_' and '-', up to 128 of them.
func ResolveRequestID(clientID string) string {
	clientID = strings.TrimSpace(clientID)
	if validRequestID(clientID) {
		return clientID
	}
	return newRequestID()
}

func validRequestID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '.' || c == '_' || c == '-':
		default:
			return false
		}
	}
	return true
}

func newRequestID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "generated-request-id"
	}
	return hex.EncodeToString(buf[:])
}
