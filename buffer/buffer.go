package buffer

import (
	"jjylik/aapari/page"
	"sync"
	"time"
)

// TODO should use a sync pool or memory arena
type BufferPool struct {
	cache    map[page.PageID]*page.Page
	mutex    sync.RWMutex
	freeList []*page.Page
	maxLen   int
}

func NewBufferPool(maxSize int) *BufferPool {
	return &BufferPool{
		// TODO make this dynamic and persistent
		freeList: make([]*page.Page, 3),
		cache:    make(map[page.PageID]*page.Page),
		maxLen:   maxSize,
	}
}

func (bp *BufferPool) GetPage(pageNum page.PageID) (*page.Page, error) {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()

	page, ok := bp.cache[pageNum]
	if ok {
		page.Accessed = time.Now()
		return page, nil
	}

	return nil, nil

}

func (bp *BufferPool) GetPages() []*page.Page {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	// TODO get rid of this allocation
	values := make([]*page.Page, 0, len(bp.cache))
	for _, value := range bp.cache {
		values = append(values, value)
	}
	return values
}

func (bp *BufferPool) AddPageToCache(page *page.Page) (evicted *page.Page) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	if len(bp.cache) == bp.maxLen {
		evicted = bp.evictLRUPage()
	}

	bp.cache[page.ID] = page
	return evicted
}

func (bp *BufferPool) AddToFreeList(page *page.Page) {
	bp.freeList = append(bp.freeList, page)
}

func (bp *BufferPool) PopFreeList() *page.Page {
	if len(bp.freeList) == 0 {
		return nil
	}
	first, rest := bp.freeList[0], bp.freeList[1:]
	bp.freeList = rest
	return first
}

func (bp *BufferPool) evictLRUPage() (evicted *page.Page) {
	var lruPage *page.Page
	for _, page := range bp.cache {
		if lruPage == nil || page.Accessed.Before(lruPage.Accessed) {
			lruPage = page
		}
	}
	return lruPage
}
