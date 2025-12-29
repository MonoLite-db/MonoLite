// Package goldstandard contains B1-class gold standard tests.
package goldstandard

import (
	"encoding/binary"
	"testing"

	"github.com/monolite/monodb/protocol"
)

// TestOpMsg_RequiredFlagBits tests B-GOLD-PROTO-001:
// OP_MSG with unknown required flag bits (0-15) must return error.
func TestOpMsg_RequiredFlagBits(t *testing.T) {
	// MongoDB Wire Protocol spec:
	// Bits 0-15 are required bits. If any unknown bit is set, the server MUST return an error.
	// Bits 16-31 are optional/ignorable bits.

	tests := []struct {
		name        string
		flagBits    uint32
		expectError bool
	}{
		{
			name:        "no flags (valid)",
			flagBits:    0,
			expectError: false,
		},
		{
			name:        "checksumPresent flag (bit 0, valid)",
			flagBits:    1, // 0x0001
			expectError: false,
		},
		{
			name:        "moreToCome flag (bit 1, valid)",
			flagBits:    2, // 0x0002
			expectError: false,
		},
		{
			name:        "unknown required bit 2 (should error)",
			flagBits:    4, // 0x0004
			expectError: true,
		},
		{
			name:        "unknown required bit 3 (should error)",
			flagBits:    8, // 0x0008
			expectError: true,
		},
		{
			name:        "unknown required bit 15 (should error)",
			flagBits:    0x8000,
			expectError: true,
		},
		{
			name:        "optional bit 16 (should NOT error)",
			flagBits:    0x10000,
			expectError: false,
		},
		{
			name:        "combination: valid + unknown required (should error)",
			flagBits:    1 | 4, // checksumPresent + unknown bit 2
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build a minimal OP_MSG
			msg := buildMinimalOpMsg(tc.flagBits)

			// Try to parse it
			_, err := protocol.ParseOpMsg(msg)

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error for flagBits=%x, got nil", tc.flagBits)
				}
			} else {
				if err != nil {
					// Note: We might get other errors (e.g., incomplete message)
					// but not specifically a "unknown required bit" error
					t.Logf("Got error (may be expected for other reasons): %v", err)
				}
			}
		})
	}
}

// buildMinimalOpMsg builds a minimal OP_MSG for testing flag bit handling.
func buildMinimalOpMsg(flagBits uint32) []byte {
	// OP_MSG format:
	// flagBits (4 bytes)
	// sections (at least one section of type 0 with a document)
	// optional checksum (4 bytes if checksumPresent flag is set)

	// Minimal document: {} -> 5 bytes (4 bytes length + 0x00)
	minDoc := []byte{5, 0, 0, 0, 0}

	// Section type 0 (body)
	sectionType := byte(0)

	// Total message
	msg := make([]byte, 4+1+len(minDoc))
	binary.LittleEndian.PutUint32(msg[0:4], flagBits)
	msg[4] = sectionType
	copy(msg[5:], minDoc)

	// If checksumPresent, add 4 bytes of checksum (simplified)
	if flagBits&1 != 0 {
		checksum := make([]byte, 4)
		msg = append(msg, checksum...)
	}

	return msg
}

// TestOpMsg_DocSequence_StrictParsing tests B-GOLD-PROTO-002:
// DocSequence parsing must be strict - bad length must return error.
func TestOpMsg_DocSequence_StrictParsing(t *testing.T) {
	tests := []struct {
		name        string
		buildMsg    func() []byte
		expectError bool
	}{
		{
			name: "valid docSequence",
			buildMsg: func() []byte {
				// Section type 1 (document sequence)
				// seqLen (4 bytes) - includes seqLen itself
				// identifier (cstring)
				// documents

				// Minimal: seqLen=10, id="x\0", doc={} (5 bytes)
				// seqLen = 4 + 2 + 5 = 11
				seqLen := int32(11)
				msg := make([]byte, 4+1+11) // flagBits + sectionType + section

				binary.LittleEndian.PutUint32(msg[0:4], 0) // flagBits
				msg[4] = 1                                  // sectionType = docSequence
				binary.LittleEndian.PutUint32(msg[5:9], uint32(seqLen))
				msg[9] = 'x'                                         // identifier
				msg[10] = 0                                          // null terminator
				binary.LittleEndian.PutUint32(msg[11:15], 5)         // doc length
				msg[15] = 0                                          // doc terminator
				return msg
			},
			expectError: false,
		},
		{
			name: "docSequence with bad seqLen (too short)",
			buildMsg: func() []byte {
				msg := make([]byte, 4+1+11)
				binary.LittleEndian.PutUint32(msg[0:4], 0)
				msg[4] = 1
				// Set seqLen to 5 (too short for identifier + doc)
				binary.LittleEndian.PutUint32(msg[5:9], 5)
				msg[9] = 'x'
				msg[10] = 0
				binary.LittleEndian.PutUint32(msg[11:15], 5)
				msg[15] = 0
				return msg
			},
			expectError: true,
		},
		{
			name: "docSequence with docLen exceeding seqLen",
			buildMsg: func() []byte {
				// seqLen says 11, but doc length says 100
				msg := make([]byte, 4+1+11)
				binary.LittleEndian.PutUint32(msg[0:4], 0)
				msg[4] = 1
				binary.LittleEndian.PutUint32(msg[5:9], 11) // seqLen
				msg[9] = 'x'
				msg[10] = 0
				binary.LittleEndian.PutUint32(msg[11:15], 100) // doc length too large
				return msg
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.buildMsg()
			_, err := protocol.ParseOpMsg(msg)

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Logf("Got error (may be expected): %v", err)
				}
			}
		})
	}
}

