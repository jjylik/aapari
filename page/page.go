package page

import (
	"bytes"
	"time"
	"unsafe"
)

const PAGE_SIZE = 4096

// TODO this shoud be dynamic, maybe
const HEADER_SIZE = 240

type PageID uint64

type Page struct {
	ID        PageID
	Contents  []byte
	KeySize   uint16
	ValueSize uint16
	FreeSlots []byte
	Next      PageID
	Accessed  time.Time
	Dirty     bool
}

type KeyValuePair struct {
	Key   *[]byte
	Value *[]byte
}

func NewPage(id PageID, keySize uint16, valueSize uint16) *Page {
	physPage := make([]byte, PAGE_SIZE)
	nextPagePlaceholder := (*PageID)(unsafe.Pointer(&physPage[0]))
	freeSlots := physPage[unsafe.Sizeof(nextPagePlaceholder):HEADER_SIZE]
	return &Page{ID: id, Contents: physPage[HEADER_SIZE:], KeySize: keySize,
		ValueSize: valueSize, FreeSlots: freeSlots, Accessed: time.Now(), Next: *nextPagePlaceholder, Dirty: false}
}

func (p *Page) findRecord(key []byte, tupleSize uint16) (value []byte, offset int64) {
	for i := int64(0); int(i) < len(p.Contents); i += int64(tupleSize) {
		chunk := p.Contents[i : i+int64(tupleSize)]
		if bytes.Equal(chunk[:p.KeySize], key) {
			return chunk[p.KeySize:], i
		}
	}
	return nil, -1
}

func (p *Page) ReadRecord(key []byte) []byte {
	tupleSize := p.KeySize + p.ValueSize
	value, _ := p.findRecord(key, tupleSize)
	return value
}

func (p *Page) findFreeSlot() int64 {
	for i := int64(0); i < int64(len(p.FreeSlots)); i++ {
		if p.FreeSlots[i] == 0 {
			return i
		}
	}
	return -1
}

func (p *Page) FilledSlotsCount() int {
	filledSlots := 0
	for i := 0; i < len(p.FreeSlots); i++ {
		if p.FreeSlots[i] == 1 {
			filledSlots++
		}
	}
	return filledSlots
}

func (p *Page) TotalSlotCount() int {
	return HEADER_SIZE
}

func (p *Page) WriteRecord(key []byte, value []byte, skipFind bool) (added bool, full bool) {
	tupleSize := p.KeySize + p.ValueSize
	offset := int64(-1)
	added = true
	if !skipFind {
		_, offset = p.findRecord(key, tupleSize)
	}
	p.Dirty = true
	if offset != -1 {
		added = false
	} else {
		freeSlotIndex := p.findFreeSlot()
		if freeSlotIndex == -1 {
			return false, true
		}
		offset = freeSlotIndex * int64(tupleSize)
		p.FreeSlots[freeSlotIndex] = byte(1)
	}
	// TODO replace with a raw pointer
	copy(p.Contents[offset:offset+int64(p.KeySize)], key)
	copy(p.Contents[offset+int64(p.KeySize):offset+int64(p.KeySize)+int64(p.ValueSize)], value)

	return added, false
}

func (p *Page) DeleteRecord(key []byte) (found bool) {
	tupleSize := p.KeySize + p.ValueSize
	_, offset := p.findRecord(key, tupleSize)
	found = offset != -1
	if found {
		p.FreeSlots[offset/int64(tupleSize)] = byte(0)
		empty := make([]byte, tupleSize)
		copy(p.Contents[offset:offset+int64(tupleSize)], empty)
		p.Dirty = true
	}
	return found
}

func (p *Page) DeleteAllRecords() {
	empty := make([]byte, len(p.FreeSlots))
	copy(p.FreeSlots, empty)
	// TODO delete also data
}

func (p *Page) ReadAllRecords() []*KeyValuePair {
	tupleSize := p.KeySize + p.ValueSize
	records := make([]*KeyValuePair, 0, len(p.FreeSlots))
	for i := int64(0); int(i) < len(p.FreeSlots); i++ {
		hasValueInSlot := p.FreeSlots[i] == 1
		if hasValueInSlot {
			offset := i * int64(tupleSize)
			key := p.Contents[offset : offset+int64(p.KeySize)]
			value := p.Contents[offset+int64(p.KeySize) : offset+int64(p.KeySize)+int64(p.ValueSize)]
			records = append(records, &KeyValuePair{&key, &value})
		}
	}
	return records
}
