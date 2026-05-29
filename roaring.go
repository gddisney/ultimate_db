package ultimate_db

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type RoaringBitmap struct {
	Chunks map[uint64][]uint16
}

func NewRoaringBitmap() *RoaringBitmap {
	return &RoaringBitmap{Chunks: make(map[uint64][]uint16)}
}

func (r *RoaringBitmap) Add(val uint64) {
	k := val >> 16
	lsb := uint16(val & 0xFFFF)
	l := r.Chunks[k]
	idx := sort.Search(len(l), func(i int) bool { return l[i] >= lsb })
	if idx < len(l) && l[idx] == lsb { return }
	l = append(l, 0)
	copy(l[idx+1:], l[idx:])
	l[idx] = lsb
	r.Chunks[k] = l
}

func (r *RoaringBitmap) ToArray() []uint64 {
	var res []uint64
	keys := make([]uint64, 0, len(r.Chunks))
	for k := range r.Chunks { keys = append(keys, k) }
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		for _, lsb := range r.Chunks[k] {
			res = append(res, (k << 16) | uint64(lsb))
		}
	}
	return res
}

func RoaringIntersect(r1, r2 *RoaringBitmap) *RoaringBitmap {
	res := NewRoaringBitmap()
	for k, l1 := range r1.Chunks {
		if l2, exists := r2.Chunks[k]; exists {
			var interl []uint16; i, j := 0, 0
			for i < len(l1) && j < len(l2) {
				if l1[i] < l2[j] { i++ } else if l1[i] > l2[j] { j++ } else { interl = append(interl, l1[i]); i++; j++ }
			}
			if len(interl) > 0 { res.Chunks[k] = interl }
		}
	}
	return res
}

func RoaringUnion(r1, r2 *RoaringBitmap) *RoaringBitmap {
	res := NewRoaringBitmap()
	// Copy r1 to res
	for k, l1 := range r1.Chunks { cl := make([]uint16, len(l1)); copy(cl, l1); res.Chunks[k] = cl }
	// Merge r2
	for k, l2 := range r2.Chunks {
		if l1, exists := res.Chunks[k]; exists {
			var unl []uint16; i, j := 0, 0
			for i < len(l1) && j < len(l2) {
				if l1[i] < l2[j] { unl = append(unl, l1[i]); i++ } else if l1[i] > l2[j] { unl = append(unl, l2[j]); j++ } else { unl = append(unl, l1[i]); i++; j++ }
			}
			unl = append(unl, l1[i:]...); unl = append(unl, l2[j:]...); res.Chunks[k] = unl
		} else { cl := make([]uint16, len(l2)); copy(cl, l2); res.Chunks[k] = cl }
	}
	return res
}

func RoaringDifference(r1, r2 *RoaringBitmap) *RoaringBitmap {
	res := NewRoaringBitmap()
	for k, l1 := range r1.Chunks {
		if l2, exists := r2.Chunks[k]; exists {
			var difl []uint16; i, j := 0, 0
			for i < len(l1) && j < len(l2) {
				if l1[i] < l2[j] { difl = append(difl, l1[i]); i++ } else if l1[i] > l2[j] { j++ } else { i++; j++ }
			}
			difl = append(difl, l1[i:]...)
			if len(difl) > 0 { res.Chunks[k] = difl }
		} else { cl := make([]uint16, len(l1)); copy(cl, l1); res.Chunks[k] = cl }
	}
	return res
}

// Add these to roaring.go

func (r *RoaringBitmap) Serialize() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(len(r.Chunks)))
	for k, list := range r.Chunks {
		binary.Write(&buf, binary.LittleEndian, k)
		binary.Write(&buf, binary.LittleEndian, uint32(len(list)))
		for _, v := range list { binary.Write(&buf, binary.LittleEndian, v) }
	}
	return buf.Bytes()
}

func (r *RoaringBitmap) Deserialize(data []byte) error {
	if len(data) == 0 { return nil }
	reader := bytes.NewReader(data)
	var chunkCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &chunkCount); err != nil { return err }
	r.Chunks = make(map[uint64][]uint16, chunkCount)
	for i := uint32(0); i < chunkCount; i++ {
		var k uint64; var listLen uint32
		binary.Read(reader, binary.LittleEndian, &k)
		binary.Read(reader, binary.LittleEndian, &listLen)
		list := make([]uint16, listLen)
		for j := uint32(0); j < listLen; j++ { binary.Read(reader, binary.LittleEndian, &list[j]) }
		r.Chunks[k] = list
	}
	return nil
}

// Add this so you can call len(res) in your tests
func (r *RoaringBitmap) Len() int {
    count := 0
    for _, list := range r.Chunks { count += len(list) }
    return count
}
