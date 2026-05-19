package ultimate_db

import (
	"errors"
	"fmt"
	"sync"
)

// Reusable buffer pool to eliminate GC pressure during decompression
var snappyDecodePool = sync.Pool{
	New: func() any {
		b := make([]byte, MaxBlockSize*3+3)
		return &b
	},
}

// Decompress reverses the Middle-Out compression strategy (Entropy & LRE)
func Decompress(src []byte, dst []byte) (int, error) {
	if len(src) < 6 {
		return 0, errors.New("source payload too small")
	}

	magic := uint16(src[0])<<8 | uint16(src[1])
	if magic != MagicHeader {
		return 0, errors.New("invalid magic header")
	}

	origLen := int(src[2])<<8 | int(src[3])
	snappyLen := int(src[4])<<8 | int(src[5])

	if len(dst) < origLen {
		return 0, fmt.Errorf("destination buffer too small: need %d bytes", origLen)
	}

	bufPtr := snappyDecodePool.Get().(*[]byte)
	snappyBuffer := *bufPtr
	defer snappyDecodePool.Put(bufPtr)
	
	if snappyLen > len(snappyBuffer) {
		return 0, errors.New("corrupted payload size exceeds max block bounds")
	}
	snappyPayload := snappyBuffer[:snappyLen]

	srcIdx := 6
	snappyIdx := 0
	var bitBuffer uint64 = 0
	var bitCount byte = 0

	for snappyIdx < snappyLen {
		for bitCount < LookaheadBits && srcIdx < len(src) {
			bitBuffer = (bitBuffer << 8) | uint64(src[srcIdx])
			bitCount += 8
			srcIdx++
		}

		if bitCount == 0 {
			break
		}

		shift := bitCount - LookaheadBits
		var lookahead uint16
		if shift >= 0 {
			lookahead = uint16((bitBuffer >> shift) & LookaheadMask)
		} else {
			lookahead = uint16((bitBuffer << -shift) & LookaheadMask)
		}

		entry := GlobalForestTable[lookahead]
		consumed := entry.Consumed()

		if consumed > 0 && consumed <= LookaheadBits {
			snappyPayload[snappyIdx] = entry.Literal()
			snappyIdx++
			bitCount -= consumed
			bitBuffer &= (1 << bitCount) - 1
		} else {
			var currNode int16 = 0
			var symbol byte
			found := false

			tempBitCount := bitCount
			tempBitBuffer := bitBuffer

			for {
				if tempBitCount == 0 {
					if srcIdx >= len(src) {
						return 0, errors.New("unexpected end of stream in sparse path")
					}
					tempBitBuffer = (tempBitBuffer << 8) | uint64(src[srcIdx])
					tempBitCount += 8
					srcIdx++
				}

				bit := byte((tempBitBuffer >> (tempBitCount - 1)) & 1)
				tempBitCount--
				// FIX: removed bitCount-- to prevent uint8 underflow

				treeIdx := int(currNode) + int(bit)
				nextNode := GlobalOverflowTree[treeIdx]

				if nextNode < 0 {
					symbol = byte(^nextNode)
					found = true
					bitBuffer = tempBitBuffer & ((1 << tempBitCount) - 1)
					bitCount = tempBitCount // FIX: Sync bitCount exactly here
					break
				}
				currNode = nextNode
			}

			if !found {
				return 0, errors.New("failed to decode sparse path")
			}

			snappyPayload[snappyIdx] = symbol
			snappyIdx++
		}
	}

	sIdx, dIdx := 0, 0
	for sIdx < len(snappyPayload) {
		if dIdx >= len(dst) {
			return 0, errors.New("decompression output exceeds destination bounds")
		}

		if snappyPayload[sIdx] == 0xFE {
			if sIdx+2 >= len(snappyPayload) {
				return 0, errors.New("truncated RLE sequence")
			}
			runLen := int(snappyPayload[sIdx+1])
			runVal := snappyPayload[sIdx+2]

			if runLen == 1 && runVal == 0xFE {
				dst[dIdx] = 0xFE
				dIdx++
			} else {
				if dIdx+runLen > len(dst) {
					return 0, errors.New("RLE expansion exceeds destination bounds")
				}
				for i := 0; i < runLen; i++ {
					dst[dIdx+i] = runVal
				}
				dIdx += runLen
			}
			sIdx += 3
		} else {
			dst[dIdx] = snappyPayload[sIdx]
			dIdx++
			sIdx++
		}
	}

	return dIdx, nil
}
