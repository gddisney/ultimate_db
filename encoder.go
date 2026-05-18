package ultimate_db

import (
	"errors"
	"fmt"
)

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

		// Fix Collision Vulnerability
		if src[sIdx] == 0xFE {
			if dIdx+2 >= len(snappyBuffer) {
				return 0, errors.New("scratch buffer overflow during literal escape transformation")
			}
			snappyBuffer[dIdx] = 0xFE
			snappyBuffer[dIdx+1] = 0x01
			snappyBuffer[dIdx+2] = 0xFE
			dIdx += 3
			sIdx++
			continue
		}

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
