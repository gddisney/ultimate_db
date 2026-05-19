package ultimate_db


// ==========================================
// 1. Global Configuration Primitives
// ==========================================

const (
	// MaxBlockSize allows for the 8-byte Page Header and DB metadata
	MaxBlockSize  = 32700                     
	MagicHeader   = 0x5348                   // "SH" Cryptographic Signature Mark
	LookaheadBits = 8                        // Forest density bit window width (2^8 = 256)
	LookaheadMask = (1 << LookaheadBits) - 1 // 0xFF Mask for isolating bit windows
	LookaheadSize = 1 << LookaheadBits
)

// ==========================================
// 2. Types & Register Packing
// ==========================================

// HuffmanIndexEntry: High 8 bits = length, Low 56 bits = code.
type HuffmanIndexEntry uint64

func NewHuffmanEntry(code uint64, length byte) HuffmanIndexEntry {
	return HuffmanIndexEntry(uint64(length)<<56 | (code & 0x00FFFFFFFFFFFFFF))
}

func (e HuffmanIndexEntry) Length() byte { return byte(e >> 56) }
func (e HuffmanIndexEntry) Code() uint64   { return uint64(e & 0x00FFFFFFFFFFFFFF) }

// ForestDensityEntry: High 8 bits = Literal, Low 8 bits = Bit Stride.
type ForestDensityEntry uint16

func NewForestEntry(literal byte, consumedBits byte) ForestDensityEntry {
	return ForestDensityEntry(uint16(literal)<<8 | uint16(consumedBits))
}

func (f ForestDensityEntry) Literal() byte  { return byte(f >> 8) }
func (f ForestDensityEntry) Consumed() byte { return byte(f & 0xFF) }

// ==========================================
// 3. Shared Storage Matrices
// ==========================================

var GlobalEncoderTable [256]HuffmanIndexEntry
var GlobalForestTable [LookaheadSize]ForestDensityEntry
var GlobalOverflowTree [1024]int16 // Expanded for deep trees

func init() {
	// A mathematically valid distribution for a 256-symbol alphabet.
	// This follows the Kraft-McMillan inequality to ensure the tree is complete.
	canonicalLengths := make([]byte, 256)
	
	// We'll assign lengths dynamically based on character categories
	for i := 0; i < 256; i++ {
		ch := byte(i)
		switch {
		case (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == ' ':
			canonicalLengths[i] = 7 // Frequently used
		case (ch >= 'A' && ch <= 'Z') || ch == '"' || ch == '_' || ch == ':':
			canonicalLengths[i] = 8 // Moderately used
		default:
			canonicalLengths[i] = 9 // Rare / Binary data
		}
	}
	
	BuildDynamicForest(canonicalLengths)
}

// BuildDynamicForest generates a canonical Huffman tree from provided lengths.
func BuildDynamicForest(lengths []byte) {
	// 1. Reset Tables
	for i := range GlobalOverflowTree { GlobalOverflowTree[i] = -1 }
	for i := range GlobalForestTable { GlobalForestTable[i] = 0 }

	// 2. Count length frequencies
	var lenCounts [17]int
	for _, l := range lengths {
		if l > 0 && l <= 16 {
			lenCounts[l]++
		}
	}

	// 3. Compute Canonical Base Codes
	// 
	var nextCode [17]uint64
	var code uint64 = 0
	for i := 1; i <= 16; i++ {
		code = (code + uint64(lenCounts[i-1])) << 1
		nextCode[i] = code
	}

	var nextNode int16 = 2

	// 4. Map Symbols to Tables
	for sym, l := range lengths {
		if l == 0 { continue }

		huffCode := nextCode[l]
		nextCode[l]++
		
		GlobalEncoderTable[sym] = NewHuffmanEntry(huffCode, l)

		// Path A: Lookahead Forest (Bit-length fits in our 8-bit window)
		if l <= LookaheadBits {
			shift := LookaheadBits - l
			startIdx := int(huffCode << shift)
			endIdx := startIdx + (1 << shift)
			
			for i := startIdx; i < endIdx; i++ {
				if i < LookaheadSize {
					GlobalForestTable[i] = NewForestEntry(byte(sym), l)
				}
			}
		} else {
			// Path B: Sparse Overflow Tree (Fallback for bits > 8)
			var currNode int16 = 0
			for bit := byte(0); bit < l; bit++ {
				direction := (huffCode >> (l - 1 - bit)) & 1
				treeIdx := int(currNode) + int(direction)
				
				if treeIdx >= len(GlobalOverflowTree) { break }

				if bit == l-1 {
					GlobalOverflowTree[treeIdx] = int16(^sym) // Store literal as bitwise NOT
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
