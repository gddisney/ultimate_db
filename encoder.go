package ultimate_db

import (
	"errors"
	"fmt"
)

// Global Configuration Primitives
const (
	MaxBlockSize  = 32768                    // 32KB Dedicated Page Boundary
	MagicHeader   = 0x5348                   // "SH" Cryptographic Signature Mark
	LookaheadBits = 8                        // Forest density bit window width
	LookaheadMask = (1 << LookaheadBits) - 1 // 0xFF Mask for isolating bit windows
)

// HuffmanIndexEntry packs variable-length codes into a single register word.
// High 8 bits: code bit-length. Low 56 bits: actual bit path code.
type HuffmanIndexEntry uint64

func NewHuffmanEntry(code uint64, length byte) HuffmanIndexEntry {
	return HuffmanIndexEntry(uint64(length)<<56 | (code & 0x00FFFFFFFFFFFFFF))
}

func (e HuffmanIndexEntry) Length() byte {
	return byte(e >> 56)
}

func (e HuffmanIndexEntry) Code() uint64 {
	return uint64(e & 0x00FFFFFFFFFFFFFF)
}

// ForestDensityEntry optimizes decoding paths into 16-bit register segments.
// High 8 bits: Literal byte value. Low 8 bits: Consumed bit-stride length.
type ForestDensityEntry uint16

func NewForestEntry(literal byte, consumedBits byte) ForestDensityEntry {
	return ForestDensityEntry(uint16(literal)<<8 | uint16(consumedBits))
}

func (f ForestDensityEntry) Literal() byte {
	return byte(f >> 8)
}

func (f ForestDensityEntry) Consumed() byte {
	return byte(f & 0xFF)
}

// Shared Package-Level Storage Matrices
var GlobalEncoderTable [256]HuffmanIndexEntry
var GlobalForestTable  [256]ForestDensityEntry
var GlobalOverflowTree [512]int16

func init() {
	// Static frequency map optimized for OIDC records, DHT assets, and GML templates
	canonicalLengths := make([]byte, 256)
	for i := 0; i < 256; i++ {
		ch := byte(i)
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == ':' || ch == '"' || ch == '_' {
			canonicalLengths[i] = 5 // High-density anchors get short paths
		} else if (ch >= 'A' && ch <= 'Z') || ch == '{' || ch == '}' || ch == ',' || ch == '[' || ch == ']' {
			canonicalLengths[i] = 7 // Medium structural wrappers
		} else {
			canonicalLengths[i] = 9 // Escapes and raw binary data
		}
	}
	buildStaticForestTables(canonicalLengths)
}

func buildStaticForestTables(lengths []byte) {
	for i := range GlobalOverflowTree {
		GlobalOverflowTree[i] = -1
	}

	var lenCounts [17]int
	for _, l := range lengths {
		if l > 0 {
			lenCounts[l]++
		}
	}

	// Track canonical base metrics
	var nextCode [17]uint64
	var code uint64 = 0
	for i := 1; i <= 16; i++ {
		code = (code + uint64(lenCounts[i-1])) << 1
		nextCode[i] = code
	}

	var nextNode int16 = 2

	for sym, l := range lengths {
		if l == 0 {
			continue
		}
		huffCode := nextCode[l]
		nextCode[l]++
		GlobalEncoderTable[sym] = NewHuffmanEntry(huffCode, l)

		if l <= LookaheadBits {
			// Dense Path: Replicate token across all matching lookahead bit prefixes
			shift := LookaheadBits - l
			startIdx := huffCode << shift
			endIdx := startIdx + (1 << shift)
			for i := startIdx; i < endIdx; i++ {
				GlobalForestTable[i] = NewForestEntry(byte(sym), l)
			}
		} else {
			// Sparse Path: Fallback binary routing map for entries > 8 bits
			var currNode int16 = 0
			for bit := byte(0); bit < l; bit++ {
				direction := (huffCode >> (l - 1 - bit)) & 1
				treeIdx := int(currNode) + int(direction)
				if bit == l-1 {
					GlobalOverflowTree[treeIdx] = int16(^sym)
				} else {
					if GlobalOverflowTree[treeIdx] == -1 {
						GlobalOverflowTree[treeIdx] = nextNode
						nextNode += 2
					}
					currNode = GlobalOverflowTree[treeIdx]
				}
			}
		}
	}
}

// Compress runs Pass 1 (High-Velocity LRE) and Pass 2 (Bit-Packed Huffman) directly to storage pointers
func Compress(src []byte, dst []byte) (int, error) {
	if len(src) == 0 {
		return 0, errors.New("empty write source payload block")
	}
	if len(src) > MaxBlockSize {
		return 0, fmt.Errorf("payload breaks max execution block size of %d", MaxBlockSize)
	}
	if len(dst) < 6 {
		return 0, errors.New("destination database page boundary under-allocated")
	}

	// Marshal structural tracking coordinates directly to page header
	dst[0] = byte(MagicHeader >> 8)
	dst[1] = byte(MagicHeader)
	dst[2] = byte(len(src) >> 8)
	dst[3] = byte(len(src))

	// Pass 1: Word-aligned byte-banging run execution
	// Allocate adequate headroom padding to completely neutralize low-entropy overflow bounds panics
	snappyBuffer := make([]byte, len(src)*3+3)
	sIdx, dIdx := 0, 0

	for sIdx < len(src) {
		// Handle Repeating Sequential Runs
		if sIdx <= len(src)-4 {
			if src[sIdx] == src[sIdx+1] && src[sIdx] == src[sIdx+2] && src[sIdx] == src[sIdx+3] {
				runVal := src[sIdx]
				runLen := 0
				for sIdx < len(src) && src[sIdx] == runVal && runLen < 255 {
					runLen++
					sIdx++
				}
				// Defensive guard constraint validating scratchpad capacity array boundaries
				if dIdx+2 >= len(snappyBuffer) {
					return 0, errors.New("scratch buffer overflow during execution run compression")
				}
				snappyBuffer[dIdx] = 0xFE // Run token escape character
				snappyBuffer[dIdx+1] = byte(runLen)
				snappyBuffer[dIdx+2] = runVal
				dIdx += 3
				continue
			}
		}

		// Fix Collision Vulnerability: Escape natural standalone 0xFE bytes to ensure decoder deterministic decoding
		if src[sIdx] == 0xFE {
			if dIdx+2 >= len(snappyBuffer) {
				return 0, errors.New("scratch buffer overflow during literal escape transformation")
			}
			snappyBuffer[dIdx] = 0xFE // Escape token
			snappyBuffer[dIdx+1] = 0x01 // Declared run length of 1
			snappyBuffer[dIdx+2] = 0xFE // Literal byte representation
			dIdx += 3
			sIdx++
			continue
		}

		// Pass-through Standard Independent Data Literals
		if dIdx >= len(snappyBuffer) {
			return 0, errors.New("scratch buffer bounds exceeded during data layout migration")
		}
		snappyBuffer[dIdx] = src[sIdx]
		dIdx++
		sIdx++
	}
	snappyPayload := snappyBuffer[:dIdx]

	dst[4] = byte(len(snappyPayload) >> 8)
	dst[5] = byte(len(snappyPayload))

	// Pass 2: High-Density Index-Keyed Entropy Compression
	dstIdx := 6
	var bitBuffer uint64 = 0
	var bitCount byte = 0

	for _, litByte := range snappyPayload {
		entry := GlobalEncoderTable[litByte]
		codeLen := entry.Length()
		codeBits := entry.Code()

		bitBuffer = (bitBuffer << codeLen) | codeBits
		bitCount += codeLen

		for bitCount >= 8 {
			if dstIdx >= len(dst) {
				return 0, errors.New("page memory limits overflowed during compression write")
			}
			shift := bitCount - 8
			dst[dstIdx] = byte(bitBuffer >> shift)
			dstIdx++
			bitCount -= 8
		}
	}

	if bitCount > 0 {
		if dstIdx >= len(dst) {
			return 0, errors.New("page memory limits overflowed during stream flush")
		}
		dst[dstIdx] = byte(bitBuffer << (8 - bitCount))
		dstIdx++
	}

	return dstIdx, nil
}
