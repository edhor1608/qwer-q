package broker

import "github.com/jonas/qwer-q/internal/types"

// Message is an alias for types.Message.
type Message = types.Message

// NewULID generates a new ULID.
func NewULID() string {
	return types.NewULID()
}
