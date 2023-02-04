package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"hash/fnv"
	"jjylik/aapari/buffer"
	"jjylik/aapari/page"
	"os"
	"time"
	"unsafe"
)

type DB struct {
	file                *os.File
	pageBuffer          *buffer.BufferPool
	threshold           float64
	meta                meta
	bucketToPage        []page.PageID
	maxRecordsPerBucket uint64
}

type meta struct {
	buckets      uint64
	splitPointer uint64
	recordCount  uint64
	pages        uint64
	keySize      uint16
	valueSize    uint16
}

const META_PAGE_SIZE = 4096

func Open(initialBucketCount uint64, pageBufferSize int, threshold float64, filePath string) (*DB, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fd, err := os.Create(filePath)
		if err != nil {
			return nil, err
		}
		// TODO check is this an efficient way to truncate file
		fileSize := int64(4096 * (initialBucketCount + 1))
		_, err = fd.Seek(fileSize-1, 0)
		if err != nil {
			return nil, err
		}
		_, err = fd.Write([]byte{0})
		if err != nil {
			return nil, err
		}
		bucketToPage := make([]page.PageID, 200) //TODO make this dynamic
		for i := 0; i < int(initialBucketCount); i++ {
			bucketToPage[i] = page.PageID(i + 1)
		}
		// TODO make these configurable
		keySize := uint16(8)
		valueSize := uint16(8)
		return &DB{
			file:       fd,
			pageBuffer: buffer.NewBufferPool(pageBufferSize),
			threshold:  threshold,
			meta: meta{
				splitPointer: 1,
				buckets:      initialBucketCount,
				recordCount:  0,
				keySize:      keySize,
				valueSize:    valueSize,
				pages:        initialBucketCount,
			},
			bucketToPage:        bucketToPage,
			maxRecordsPerBucket: uint64((META_PAGE_SIZE - page.HEADER_SIZE) / (keySize + valueSize)),
		}, nil
	} else {
		fd, err := os.OpenFile(filePath, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		metaPage := make([]byte, META_PAGE_SIZE)
		_, err = fd.ReadAt(metaPage, 0)
		if err != nil {
			return nil, err
		}
		meta := (*meta)(unsafe.Pointer(&metaPage[0]))
		// TODO better padding to 2^x
		metaSize := unsafe.Sizeof(*meta) + 8

		// TODO add custom decoder
		bucketToPageBytes := metaPage[metaSize : META_PAGE_SIZE-1]
		buf := bytes.NewBuffer(bucketToPageBytes)
		dec := gob.NewDecoder(buf)
		bucketToPage := make([]page.PageID, 0)
		err = dec.Decode(&bucketToPage)
		if err != nil {
			return nil, err
		}
		return &DB{
			file:                fd,
			pageBuffer:          buffer.NewBufferPool(16),
			threshold:           threshold,
			meta:                *meta,
			bucketToPage:        bucketToPage,
			maxRecordsPerBucket: uint64((META_PAGE_SIZE - page.HEADER_SIZE) / (meta.keySize + meta.valueSize)),
		}, nil
	}

}

func (db *DB) readPageFromDisk(id page.PageID) (*page.Page, error) {
	physPage := make([]byte, page.PAGE_SIZE)
	_, err := db.file.ReadAt(physPage, int64((id)*page.PAGE_SIZE))
	if err != nil {
		return nil, err
	}
	nextPage := (*page.PageID)(unsafe.Pointer(&physPage[0]))
	freeSlots := physPage[unsafe.Sizeof(nextPage):page.HEADER_SIZE]
	return &page.Page{ID: id, Contents: physPage[page.HEADER_SIZE:],
		KeySize: uint16(db.meta.keySize), ValueSize: uint16(db.meta.valueSize),
		FreeSlots: freeSlots, Accessed: time.Now(), Next: *nextPage}, nil
}

func (db *DB) writeToDisk(p *page.Page) error {
	// TODO this should be removed, keep the full byte array in memory
	newPage := append(p.FreeSlots, p.Contents...)
	var b [8]byte
	ptr := (*[8]byte)(unsafe.Pointer(&p.Next))
	// TODO is this copy needed?
	copy(b[:], ptr[:])
	newPage = append(b[:], newPage...)
	_, err := db.file.WriteAt(newPage, (int64(p.ID))*page.PAGE_SIZE)
	return err
}

// TODO this is just wrong and ugly
func (db *DB) writeMeta() error {
	meta := db.meta
	_, err := db.file.Seek(0, 0)
	if err != nil {
		return err
	}
	if err := binary.Write(db.file, binary.LittleEndian, meta); err != nil {
		return err
	}
	// TODO better padding to 2^x
	_, err = db.file.Seek(int64(unsafe.Sizeof(meta)+8), 0)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	// TODO this should not be hard coded to single page
	err = enc.Encode(db.bucketToPage)
	// TODO add freelist
	if err != nil {
		return err
	}
	_, err = db.file.Write(buf.Bytes())
	return err

}

func (db *DB) ensureCapacity() error {
	_, err := db.file.Seek(int64(db.meta.pages)-1, 0)
	if err != nil {
		return err
	}
	_, err = db.file.Write([]byte{0})
	return err
}

func (db *DB) getRootPage(bucket int64) (*page.Page, error) {
	pageId := db.bucketToPage[uint64(bucket)]
	return db.getPageWithId(pageId)
}

func (db *DB) getPageWithId(pageId page.PageID) (*page.Page, error) {
	p, err := db.pageBuffer.GetPage(pageId)
	if err != nil {
		return nil, err
	}
	if p == nil {
		p, err := db.readPageFromDisk(pageId)
		if err != nil {
			return nil, err
		}
		err = db.addToPageBuffer(p)
		if err != nil {
			return nil, err
		}
		return p, nil
	}
	return p, nil
}

func padKey(key []byte, keySize uint16) []byte {
	keyPad := make([]byte, keySize)
	copy(keyPad, key)
	return keyPad
}

func (db *DB) addToPageBuffer(newPage *page.Page) error {
	evicted := db.pageBuffer.AddPageToCache(newPage)
	if evicted != nil && evicted.Dirty {
		err := db.writeToDisk(evicted)
		return err
	}
	return nil
}

func (db *DB) allocateNewPage() (*page.Page, error) {
	freeListPage := db.pageBuffer.PopFreeList()
	if freeListPage != nil {
		return freeListPage, nil
	}
	db.meta.pages += 1
	newPage := page.NewPage(page.PageID(db.meta.pages), db.meta.keySize, db.meta.valueSize)
	err := db.ensureCapacity()
	if err != nil {
		return nil, err
	}
	err = db.addToPageBuffer(newPage)
	if err != nil {
		return nil, err
	}
	return newPage, nil
}

func (db *DB) handleInsert(pageId page.PageID, key []byte, value []byte, overwrite bool) (added bool, err error) {
	current := pageId
	full := false
	// TODO tidy this up
	for current != 0 {
		p, err := db.getPageWithId(current)
		if err != nil {
			return false, err
		}
		added, full = p.WriteRecord(key, value, overwrite)
		if full && p.Next == 0 {
			newPage, err := db.allocateNewPage()
			if err != nil {
				return false, err
			}
			p.Next = newPage.ID
			current = newPage.ID
		} else if full {
			current = p.Next
		} else {
			break
		}
	}
	return added, nil
}

func (db *DB) writeDirtyPages() error {
	// TODO inefficient
	for _, p := range db.pageBuffer.GetPages() {
		if p.Dirty {
			err := db.writeToDisk(p)
			if err != nil {
				return err
			}
			p.Dirty = false
		}
	}
	return nil
}

func (db *DB) Put(key []byte, value []byte) (err error) {
	if len(value) > int(db.meta.valueSize) {
		return errors.New("value too large")
	}
	if len(key) > int(db.meta.keySize) {
		return errors.New("key too large")
	}
	safeKey := padKey(key, db.meta.keySize)
	bucket := db.getBucket(safeKey)
	added, err := db.handleInsert(db.bucketToPage[uint64(bucket)], safeKey, value, false)
	if err != nil {
		return err
	}
	if added {
		db.meta.recordCount++
	}
	if float64(db.meta.recordCount) > db.threshold*float64(db.meta.buckets*db.maxRecordsPerBucket) {
		err := db.grow()
		if err != nil {
			return err
		}
	}
	err = db.writeDirtyPages()
	if err != nil {
		return err
	}
	return nil
}

func lsbMask(n uint64) uint64 {
	return (1 << n) - 1
}

func (db *DB) getBucket(key []byte) int64 {
	hash := hash(key)
	mask := lsbMask(db.meta.splitPointer)
	bucket := (hash & mask)
	if bucket >= db.meta.buckets {
		bucket = bucket ^ (1 << (db.meta.splitPointer - 1))
	}
	return int64(bucket)
}

func (db *DB) findValue(pageId page.PageID, key []byte) ([]byte, error) {
	current := pageId
	for current != 0 {
		p, err := db.getPageWithId(current)
		if err != nil {
			return nil, err
		}
		value := p.ReadRecord(key)
		if value == nil && p.Next != 0 {
			current = p.Next
		} else {
			return value, nil
		}
	}
	return nil, nil
}

// TODO copy-paste from find
func (db *DB) deleteValue(pageId page.PageID, key []byte) (bool, error) {
	current := pageId
	for current != 0 {
		p, err := db.getPageWithId(current)
		if err != nil {
			return false, err
		}
		found := p.DeleteRecord(key)
		if found {
			return true, nil
		} else {
			current = p.Next
		}
	}
	return false, nil
}

func (db *DB) Get(key []byte) (result []byte, found bool, err error) {
	if len(key) > int(db.meta.keySize) {
		return nil, false, errors.New("key too large")
	}
	safeKey := padKey(key, db.meta.keySize)
	bucket := db.getBucket(safeKey)
	value, err := db.findValue(db.bucketToPage[uint64(bucket)], safeKey)
	if err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, false, nil
	}
	result = make([]byte, len(value))
	copy(result, value)
	return result, true, nil
}

func (db *DB) Delete(key []byte) (bool, error) {
	if len(key) > int(db.meta.keySize) {
		return false, errors.New("key too large")
	}
	safeKey := padKey(key, db.meta.keySize)
	bucket := db.getBucket(key)
	ok, err := db.deleteValue(db.bucketToPage[uint64(bucket)], safeKey)
	if err != nil {
		return false, err
	}
	if ok {
		err = db.writeDirtyPages()
		if err != nil {
			return false, err
		}
	}
	return ok, nil
}

func (db *DB) readAndDelete(rootPageId page.PageID) (records []*page.KeyValuePair, err error) {
	current := rootPageId
	for current != 0 {
		p, err := db.getPageWithId(current)
		if err != nil {
			return nil, err
		}
		records = append(records, p.ReadAllRecords()...)
		p.DeleteAllRecords()
		current = p.Next
		p.Dirty = true
		p.Next = 0
		if p.ID != rootPageId {
			db.pageBuffer.AddToFreeList(p)
		}
	}
	return records, nil
}

func (db *DB) grow() error {
	bucket_to_split := int64(db.meta.buckets % (1 << (db.meta.splitPointer - 1)))
	p, err := db.getRootPage(bucket_to_split)
	if err != nil {
		return err
	}
	db.meta.buckets += 1
	newPage, err := db.allocateNewPage()
	if err != nil {
		return err
	}
	db.bucketToPage[db.meta.buckets-1] = newPage.ID
	if db.meta.buckets > (1 << db.meta.splitPointer) {
		db.meta.splitPointer += 1
	}
	records, err := db.readAndDelete(p.ID)
	if err != nil {
		return err
	}
	for _, record := range records {
		bucket := db.getBucket(*record.Key)
		if bucket == bucket_to_split {
			_, err := db.handleInsert(p.ID, *record.Key, *record.Value, true)
			if err != nil {
				return err
			}
		} else {
			_, err := db.handleInsert(newPage.ID, *record.Key, *record.Value, true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) Close() error {
	if db != nil && db.file != nil {
		err := db.writeMeta()
		if err != nil {
			return err
		}
		err = db.file.Close()
		return err
	}
	return nil
}

func hash(b []byte) uint64 {
	hash := fnv.New64a()
	hash.Write(b)
	return hash.Sum64()
}
