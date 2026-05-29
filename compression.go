package ultimate_db

import (
	"errors"
	"sync"
)


// ==========================================
// 2. Types & Register Packing
// ==========================================

type HuffmanIndexEntry uint64

func NewHuffmanEntry(code uint64, length byte) HuffmanIndexEntry {
	return HuffmanIndexEntry(uint64(length)<<56 | (code & 0x00FFFFFFFFFFFFFF))
}

func (e HuffmanIndexEntry) Length() byte { return byte(e >> 56) }
func (e HuffmanIndexEntry) Code() uint64 { return uint64(e & 0x00FFFFFFFFFFFFFF) }

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
var GlobalOverflowTree [1024]int16

func init() {
	canonicalLengths := make([]byte, 256)
	for i := 0; i < 256; i++ {
		ch := byte(i)
		switch {
		case (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == ' ':
			canonicalLengths[i] = 7
		case (ch >= 'A' && ch <= 'Z') || ch == '"' || ch == '_' || ch == ':':
			canonicalLengths[i] = 8
		default:
			canonicalLengths[i] = 9
		}
	}
	BuildDynamicForest(canonicalLengths)
}

func BuildDynamicForest(lengths []byte) {
	for i := range GlobalOverflowTree { GlobalOverflowTree[i] = -1 }
	for i := range GlobalForestTable { GlobalForestTable[i] = 0 }

	var lenCounts [17]int
	for _, l := range lengths {
		if l > 0 && l <= 16 { lenCounts[l]++ }
	}

	var nextCode [17]uint64
	var code uint64 = 0
	for i := 1; i <= 16; i++ {
		code = (code + uint64(lenCounts[i-1])) << 1
		nextCode[i] = code
	}

	var nextNode int16 = 2
	for sym, l := range lengths {
		if l == 0 { continue }
		huffCode := nextCode[l]
		nextCode[l]++
		GlobalEncoderTable[sym] = NewHuffmanEntry(huffCode, l)

		if l <= LookaheadBits {
			shift := LookaheadBits - l
			startIdx := int(huffCode << shift)
			endIdx := startIdx + (1 << shift)
			for i := startIdx; i < endIdx; i++ {
				if i < LookaheadSize { GlobalForestTable[i] = NewForestEntry(byte(sym), l) }
			}
		} else {
			var currNode int16 = 0
			for bit := byte(0); bit < l; bit++ {
				direction := int((huffCode >> (l - 1 - bit)) & 1)
				treeIdx := int(currNode) + direction
				if treeIdx >= len(GlobalOverflowTree) { break }
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

// ==========================================
// 4. Compression Engine
// ==========================================

var snappyEncodePool = sync.Pool{New: func() any { b := make([]byte, MaxBlockSize*3+3); return &b }}
var snappyDecodePool = sync.Pool{New: func() any { b := make([]byte, MaxBlockSize*3+3); return &b }}

func Compress(src []byte, dst []byte) (int, error) {
	if len(src) == 0 || len(src) > MaxBlockSize || len(dst) < 6 {
		return 0, errors.New("invalid compression parameters")
	}

	dst[0], dst[1] = byte(MagicHeader>>8), byte(MagicHeader&0xFF)
	dst[2], dst[3] = byte(len(src)>>8), byte(len(src))

	bufPtr := snappyEncodePool.Get().(*[]byte)
	snappyBuffer := *bufPtr
	defer snappyEncodePool.Put(bufPtr)

	sIdx, dIdx := 0, 0
	for sIdx < len(src) {
		// LRE Pass
		if sIdx <= len(src)-4 && src[sIdx] == src[sIdx+1] && src[sIdx] == src[sIdx+2] && src[sIdx] == src[sIdx+3] {
			runVal, runLen := src[sIdx], 0
			for sIdx < len(src) && src[sIdx] == runVal && runLen < 255 {
				runLen++; sIdx++
			}
			snappyBuffer[dIdx], snappyBuffer[dIdx+1], snappyBuffer[dIdx+2] = 0xFE, byte(runLen), runVal
			dIdx += 3
			continue
		}
		snappyBuffer[dIdx] = src[sIdx]
		dIdx++
		sIdx++
	}

	dst[4], dst[5] = byte(dIdx>>8), byte(dIdx)
	dstIdx := 6
	var bitBuffer uint64
	var bitCount byte

	for i := 0; i < dIdx; i++ {
		entry := GlobalEncoderTable[snappyBuffer[i]]
		bitBuffer = (bitBuffer << entry.Length()) | entry.Code()
		bitCount += entry.Length()
		for bitCount >= 8 {
			dst[dstIdx] = byte(bitBuffer >> (bitCount - 8))
			dstIdx++
			bitCount -= 8
		}
	}
	if bitCount > 0 {
		dst[dstIdx] = byte(bitBuffer << (8 - bitCount))
		dstIdx++
	}
	return dstIdx, nil
}

func Decompress(src []byte, dst []byte) (int, error) {
	origLen := int(src[2])<<8 | int(src[3])
        _ = origLen // Tell Go we know it's unused if you don't need it for buffer allocation
	snappyLen := int(src[4])<<8 | int(src[5])
	snappyPayload := make([]byte, snappyLen)

	// Huffman Decode Pass
	srcIdx, snappyIdx := 6, 0
	var bitBuffer uint64
	var bitCount byte
	for snappyIdx < snappyLen {
		for bitCount < LookaheadBits && srcIdx < len(src) {
			bitBuffer = (bitBuffer << 8) | uint64(src[srcIdx])
			bitCount += 8
			srcIdx++
		}
		
		lookahead := uint16(bitBuffer >> (bitCount - LookaheadBits)) & LookaheadMask
		entry := GlobalForestTable[lookahead]

		if entry.Consumed() > 0 {
			snappyPayload[snappyIdx] = entry.Literal()
			snappyIdx++
			bitCount -= entry.Consumed()
		} else {
			// Sparse tree walk... (Implement tree walk here)
			return 0, errors.New("sparse path not implemented") 
		}
	}

	// LRE Expand Pass
	dIdx := 0
	for sIdx := 0; sIdx < len(snappyPayload); sIdx++ {
		if snappyPayload[sIdx] == 0xFE {
			runLen, runVal := int(snappyPayload[sIdx+1]), snappyPayload[sIdx+2]
			for i := 0; i < runLen; i++ { dst[dIdx+i] = runVal }
			dIdx += runLen
			sIdx += 2
		} else {
			dst[dIdx] = snappyPayload[sIdx]
			dIdx++
		}
	}
	return dIdx, nil
}
