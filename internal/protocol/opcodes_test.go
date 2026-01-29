package protocol

import "testing"

func TestOpCodeValues(t *testing.T) {
	// Verify opcodes don't accidentally change
	if OpPublish != 0x01 {
		t.Errorf("OpPublish = %x, want 0x01", OpPublish)
	}
	if OpError != 0x07 {
		t.Errorf("OpError = %x, want 0x07", OpError)
	}
}

func TestOpCodeString(t *testing.T) {
	if OpPublish.String() != "PUBLISH" {
		t.Errorf("OpPublish.String() = %s, want PUBLISH", OpPublish.String())
	}
}
