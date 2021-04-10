package blockcache

import (
	"container/list"
	"sync"
)

type Flushable interface {
	FlushBlock(key interface{}, buf []byte) error
}

type cacheVal struct {
	Buf []byte // Guarded by val.Lock

	// Marks that the cache value is no longer alive. Do to the locking scheme it
	// is possible a value is no longer good after you retrieve it from the cache
	// so we use this flag to mark if we should refresh our value.
	Dead bool // Guarded by val.Lock

	// Locks access to this cache item. This should be locked anytime a client is
	// accessing the data at this cache item.
	Lock sync.Mutex

	// Can be read holding cache.Lock or val.Lock, must hold both to write.
	OldElem   *list.Element
	DirtyElem *list.Element

	// Immutable upon creation
	GroupKey interface{}
	SubKey   interface{}
}

type BlockCache struct {
	// Current number of elements in the cache
	Size int

	// Maximum number of elements in the cache.
	CacheSize int

	// Size of block allocation for cache elements.
	BlockSize int

	Pool sync.Pool

	// Global block cache lock. Never hold the global lock while holding a value
	// lock. The opposite is permissable. Never block or invoke callbacks while
	// holding this lock, it is meant to be a short-term lock.
	lock sync.Mutex

	groupMap  map[interface{}]map[interface{}]*cacheVal
	oldList   *list.List
	dirtyList *list.List
}

func New(cacheSize, blockSize int) *BlockCache {
	return &BlockCache{
		Size:      0,
		CacheSize: cacheSize,
		BlockSize: blockSize,
		Pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, blockSize)
			},
		},
		groupMap:  make(map[interface{}]map[interface{}]*cacheVal),
		oldList:   list.New(),
		dirtyList: list.New(),
	}
}

func (c *BlockCache) deleteValue(val *cacheVal) (bool, error) {
	// Mark the value as dead.
	val.Lock.Lock()
	defer val.Lock.Unlock()

	if val.Dead {
		// Someone else already deleted/deleting this item
		return false, nil
	}

	if val.DirtyElem != nil {
		groupFlushable, ok := val.GroupKey.(Flushable)
		if ok {
			err := groupFlushable.FlushBlock(val.SubKey, val.Buf)
			if err != nil {
				return false, err
			}
		}
	}

	val.Dead = true
	c.Pool.Put(val.Buf)

	return true, nil
}

func (c *BlockCache) lookup(groupKey, key interface{}, create bool) (*cacheVal, bool, error) {
	var val *cacheVal
	var created bool

	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.evict()
	if err != nil {
		return nil, false, err
	}

	submap, ok := c.groupMap[groupKey]
	if create && !ok {
		submap = make(map[interface{}]*cacheVal)
		c.groupMap[groupKey] = submap
		ok = true
	}
	if ok {
		val, ok = submap[key]
		if create && !ok {
			c.Size++
			buf := c.Pool.Get().([]byte)
			for i := 0; i < len(buf); i++ {
				buf[i] = 0
			}
			val = &cacheVal{
				Buf:      buf,
				GroupKey: groupKey,
				SubKey:   key,
			}
			val.OldElem = c.oldList.PushBack(val)
			created = true

			submap[key] = val
		} else if ok {
			c.oldList.MoveToBack(val.OldElem)
		}
	}

	return val, created, nil
}

func (c *BlockCache) evict() error {
	for c.CacheSize <= c.Size {
		// Find the element we want to delete.
		val := c.oldList.Front().Value.(*cacheVal)
		c.lock.Unlock()

		val.Lock.Lock()

		if val.Dead {
			// Someone else already handled deleting this item, skip.
			val.Lock.Unlock()
			c.lock.Lock()
			continue
		}

		// Flush the element if dirty
		if val.DirtyElem != nil {
			groupFlushable, ok := val.GroupKey.(Flushable)
			if ok {
				err := groupFlushable.FlushBlock(val.SubKey, val.Buf)
				if err != nil {
					val.Lock.Unlock()
					c.lock.Lock()
					return err
				}
			}
		}

		// Mark the value as dead.
		val.Dead = true
		c.Pool.Put(val.Buf)

		// Regrab map lock first to ensure anyone who sees a dead value will not get
		// that dead value again if they refresh their value.
		c.lock.Lock()
		val.Lock.Unlock()

		// Delete from groupMap if still present (which it probably is unless
		// someone accessed the same key again)
		submap, ok := c.groupMap[val.GroupKey]
		if ok {
			curVal := submap[val.SubKey]
			if curVal == val {
				delete(submap, val.SubKey)
			}
		}
		if len(submap) == 0 {
			delete(c.groupMap, val.GroupKey)
		}

		// Remove from old list and dirty list.
		c.oldList.Remove(val.OldElem)
		if val.DirtyElem != nil {
			c.dirtyList.Remove(val.DirtyElem)
		}

		c.Size--
	}

	return nil
}

func (c *BlockCache) flushOne() error {
	c.lock.Lock()
	val := c.dirtyList.Front().Value.(*cacheVal)
	c.lock.Unlock()

	val.Lock.Lock()
	defer val.Lock.Unlock()

	if val.Dead || val.DirtyElem == nil {
		// Element is being deleted or already flushed, skip
		return nil
	}

	// Flush the element if dirty
	groupFlushable := val.GroupKey.(Flushable)
	err := groupFlushable.FlushBlock(val.SubKey, val.Buf)
	if err != nil {
		return err
	}

	c.lock.Lock()
	c.dirtyList.Remove(val.DirtyElem)
	val.DirtyElem = nil
	c.lock.Unlock()

	return nil
}

func (c *BlockCache) Access(groupKey, key interface{}, create bool, accessFunc func(buf []byte, found bool) (modified bool, err error)) error {
	var val *cacheVal
	var created bool
	var err error
	for {
		val, created, err = c.lookup(groupKey, key, create)
		if err != nil {
			return err
		}
		if val == nil {
			break
		}

		val.Lock.Lock()
		if val.Dead {
			val.Lock.Unlock()
			continue
		}

		defer val.Lock.Unlock()
		break
	}

	if val == nil {
		_, err := accessFunc(nil, false)
		return err
	}

	modified, err := accessFunc(val.Buf, !created)
	if modified {
		// Mark element dirty
		c.lock.Lock()
		if val.DirtyElem == nil {
			val.DirtyElem = c.dirtyList.PushBack(val)
		} else {
			c.dirtyList.MoveToBack(val.DirtyElem)
		}
		c.lock.Unlock()
	}
	return err
}

func (c *BlockCache) Flush(groupKey interface{}, key interface{}) error {
	val, _, err := c.lookup(groupKey, key, false)
	if err != nil {
		return err
	}
	if val == nil {
		return nil
	}

	val.Lock.Lock()
	defer val.Lock.Unlock()

	if val.Dead {
		return nil
	}

	groupFlushable := groupKey.(Flushable)
	err = groupFlushable.FlushBlock(key, val.Buf)

	c.lock.Lock()
	defer c.lock.Unlock()

	c.dirtyList.Remove(val.DirtyElem)
	val.DirtyElem = nil

	return err
}

func (c *BlockCache) FlushGroup(groupKey interface{}) error {
	var vals []*cacheVal

	c.lock.Lock()
	submap, ok := c.groupMap[groupKey]
	if ok {
		vals = make([]*cacheVal, 0, len(submap))
		for _, val := range submap {
			if val.DirtyElem != nil {
				vals = append(vals, val)
			}
		}
	}
	c.lock.Unlock()

	groupFlushable := groupKey.(Flushable)
	for _, val := range vals {
		val.Lock.Lock()

		if val.Dead || val.DirtyElem == nil {
			val.Lock.Unlock()
			continue
		}

		// Flush the element if dirty
		err := groupFlushable.FlushBlock(val.SubKey, val.Buf)
		if err != nil {
			val.Lock.Unlock()
			return err
		}

		// Remove from dirty list
		c.lock.Lock()
		c.dirtyList.Remove(val.DirtyElem)
		val.DirtyElem = nil
		c.lock.Unlock()

		val.Lock.Unlock()
	}

	return nil
}

func (c *BlockCache) RemoveGroup(groupKey interface{}) error {
	var vals []*cacheVal

	c.lock.Lock()
	submap, ok := c.groupMap[groupKey]
	if ok {
		vals = make([]*cacheVal, 0, len(submap))
		for _, val := range submap {
			vals = append(vals, val)
		}
	}
	c.lock.Unlock()

	groupFlushable, _ := groupKey.(Flushable)
	for _, val := range vals {
		val.Lock.Lock()

		if val.Dead || val.DirtyElem == nil {
			val.Lock.Unlock()
			continue
		}

		// Flush the element if dirty
		if groupFlushable != nil {
			err := groupFlushable.FlushBlock(val.SubKey, val.Buf)
			if err != nil {
				val.Lock.Unlock()
				return err
			}
		}

		// Remove from dirty list
		c.lock.Lock()

		val.Dead = true
		if val.DirtyElem != nil {
			c.dirtyList.Remove(val.DirtyElem)
		}
		c.oldList.Remove(val.OldElem)

		// Delete from groupMap if still present (which it probably is unless
		// someone accessed the same key again)
		submap, ok := c.groupMap[val.GroupKey]
		if ok {
			curVal := submap[val.SubKey]
			if curVal == val {
				delete(submap, val.SubKey)
			}
		}
		if len(submap) == 0 {
			delete(c.groupMap, val.GroupKey)
		}

		c.lock.Unlock()

		val.Lock.Unlock()
	}

	return nil
}
