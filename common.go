package ultimate_db

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
var GlobalForestTable [256]ForestDensityEntry
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
