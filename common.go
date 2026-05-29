package ultimate_db

// ============================================================================
// 1. Core Storage & Layout Constants
// ============================================================================

const (
	PageSize = 32768

	// Expanded to 32 bytes to comfortably seat a 4-byte CRC, 4-byte Type,
	// 4-byte Slot Count, 4-byte Lower, 4-byte Upper, and an 8-byte NextPageID pointer.
	PageHeaderSize = 32

	RecordHeaderSize = 24
	BTreeHeaderSize  = 24
)

// ============================================================================
// 2. Hardware and Node Structural Flags
// ============================================================================

const (
	PageTypeInternal = 1
	PageTypeLeaf     = 2
	PrefixHash       = "H:"
)

// ============================================================================
// 3. Compression and Safety Margins
// ============================================================================

const (
	MaxBlockSize  = 32700
	MagicHeader   = 0x5348
	LookaheadBits = 8
	LookaheadMask = 0xFF
	LookaheadSize = 256
	MaxQueryDepth = 50
)

// ============================================================================
// 4. ARIES Transaction Log Record Types
// ============================================================================

const (
	LogTypeBegin uint8 = iota
	LogTypeUpdate
	LogTypeCommit
	LogTypeAbort
	LogTypeCLR
	LogTypeCheckpoint
)

// ============================================================================
// 5. Global Type Definitions & Primitives
// ============================================================================

type PageID uint64
type LogSequenceNumber uint64

func min(a, b int) int {
	if a < b { return a }
	return b
}
