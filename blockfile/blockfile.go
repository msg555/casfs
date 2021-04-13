package blockfile

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/blockcache"
)

type BlockAllocator interface {
	GetBlockSize() int
	GetMetaDataSize() int
	GetNumBlocks() (BlockIndex, error)
	GetCache() *blockcache.BlockCache

	Close() error

	Allocate(tag interface{}) (BlockIndex, error)
	Free(index BlockIndex) error
	Read(index BlockIndex, buf []byte) ([]byte, error)
	ReadAt(index BlockIndex, off, sz int, buf []byte) ([]byte, error)
	Write(tag interface{}, index BlockIndex, buf []byte) error
	WriteAt(tag interface{}, index BlockIndex, off int, buf []byte) error
	SyncTag(tag interface{}) error
	AccessBlockMeta(index BlockIndex, accessFunc func(meta []byte) (modified bool, err error)) error
	IsBlockReadOnly(index BlockIndex) bool
}

var bo = binary.LittleEndian

type BlockIndex = int64

type BlockFile struct {
	MetaDataSize int
	Cache     *blockcache.BlockCache
	File      *os.File

	blocksPerMeta int

	allocLock sync.Mutex
	tagLock sync.Mutex
	tagDirtyBlocks map[interface{}]map[BlockIndex]struct{}
}

func readAtFull(file *os.File, offset int64, buf []byte) error {
	read := 0
	for read < len(buf) {
		n, err := file.ReadAt(buf[read:], int64(offset)+int64(read))
		if err != nil {
			return err
		}
		read += n
	}
	return nil
}

func writeAtFull(file *os.File, offset int64, buf []byte) error {
	written := 0
	for written < len(buf) {
		n, err := file.WriteAt(buf[written:], int64(offset)+int64(written))
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

// Opens the passed blockfile, creating it if necessary using `perm`
// permissions.
func (bf *BlockFile) Open(path string, perm os.FileMode) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, perm)
	if err != nil {
		return err
	}

	bf.File = file
	bf.Init()
	return nil
}

func (bf *BlockFile) Init() {
	if bf.MetaDataSize > bf.Cache.BlockSize {
		panic("metadata size exceeds block size")
	}
	if bf.MetaDataSize == 0 {
		bf.blocksPerMeta = 0
	} else {
		bf.blocksPerMeta = bf.Cache.BlockSize / bf.MetaDataSize
	}
  bf.tagDirtyBlocks = make(map[interface{}]map[BlockIndex]struct{})
}

// Closes the underlying file handle.
func (bf *BlockFile) Close() error {
	err := bf.Cache.RemoveGroup(bf)
	if err != nil {
		bf.File.Close()
		return err
	}
	return bf.File.Close()
}

func (bf * BlockFile) GetBlockSize() int {
	return bf.Cache.BlockSize
}

func (bf * BlockFile) GetMetaDataSize() int {
	return bf.MetaDataSize
}

func (bf *BlockFile) GetNumBlocks() (BlockIndex, error) {
	buf, err := bf.ReadAt(0, 8, 8, nil)
	if err != nil {
		return 0, err
	}
	return BlockIndex(bo.Uint64(buf) + 1), nil
}

func (bf * BlockFile) GetCache() *blockcache.BlockCache {
	return bf.Cache
}

// Allocate a new block and returns the index. The index is a positive int64
// that will never exceed the maximum number of simultaneously allocated blocks.
func (bf *BlockFile) Allocate(tag interface{}) (BlockIndex, error) {
	bf.allocLock.Lock()
	defer bf.allocLock.Unlock()

	buf := bf.Cache.Pool.Get().([]byte)
	defer bf.Cache.Pool.Put(buf)

	// Read in head block
	_, err := bf.Read(0, buf)
	if err != nil {
		return 0, err
	}

	// Skip to head free block if any exist.
	freeHead := BlockIndex(bo.Uint64(buf))
	if freeHead != 0 {
		_, err = bf.Read(freeHead, buf)
		if err != nil {
			return 0, err
		}
	}

	// Find open location to write block index
	lo := 1
	if freeHead == 0 {
		lo++
	}
	hi := bf.Cache.BlockSize / 8
	for lo < hi {
		md := lo + (hi-lo)/2
		if bo.Uint64(buf[md*8:]) == 0 {
			hi = md
		} else {
			lo = md + 1
		}
	}

	// If there is a free block in the current head return that.
	var dat [8]byte
	if lo > 1 {
		result := int64(bo.Uint64(buf[(lo-1)*8:]))
		if freeHead == 0 && lo == 2 {
			result++
			if bf.blocksPerMeta != 0 && result % int64(bf.blocksPerMeta) == 0 {
				// Do not assign new blocks to a meta block index.
				result++
			}
			bo.PutUint64(dat[:], uint64(result))
		}
		err = bf.WriteAt(bf, freeHead, (lo-1)*8, dat[:])
		if err != nil {
			return 0, err
		}
		err = bf.zeroBlock(tag, result)
		if err != nil {
			return 0, err
		}
		return result, nil
	}

	// Otherwise the current free head is the new free block.
	err = bf.WriteAt(bf, 0, 0, buf[:8])
	if err != nil {
		return 0, err
	}

	err = bf.zeroBlock(tag, freeHead)
	if err != nil {
		return 0, err
	}
	return freeHead, nil
}

// Free a block and allow it to be allocated in the future.
func (bf *BlockFile) Free(index BlockIndex) error {
	bf.allocLock.Lock()
	defer bf.allocLock.Unlock()

	buf := bf.Cache.Pool.Get().([]byte)
	defer bf.Cache.Pool.Put(buf)

	// Read in head block
	_, err := bf.Read(0, buf)
	if err != nil {
		return err
	}

	// Skip to head free block if any exist.
	freeHead := BlockIndex(bo.Uint64(buf))
	if freeHead != 0 {
		_, err = bf.Read(freeHead, buf)
		if err != nil {
			return err
		}
	}

	// Find open location to write block index
	lo := 1
	hi := bf.Cache.BlockSize / 8
	if freeHead == 0 {
		lo++
	}
	for lo < hi {
		md := lo + (hi-lo)/2
		if bo.Uint64(buf[md*8:]) == 0 {
			hi = md
		} else {
			lo = md + 1
		}
	}

	var dat [8]byte
	bo.PutUint64(dat[:], uint64(index))

	if lo < bf.Cache.BlockSize/8 {
		// Room for new index in current free head.
		return bf.WriteAt(bf, freeHead, lo*8, dat[:])
	}

	// Make index the new free head.
	bo.PutUint64(buf, uint64(freeHead))
	for i := 8; i < len(buf); i++ {
		buf[i] = 0
	}
	err = bf.Write(bf, index, buf)
	if err != nil {
		return err
	}

	bo.PutUint64(dat[:], uint64(index))
	return bf.WriteAt(bf, 0, 0, dat[:])
}

func (bf *BlockFile) FlushBlock(ikey interface{}, tag interface{}, buf []byte) (interface{}, error) {
	index := ikey.(BlockIndex)
	bf.updateCacheTag(index, tag, nil)
	return nil, writeAtFull(bf.File, index*int64(bf.Cache.BlockSize), buf)
}

// Read an entire block into the passed buffer. If the buffer is not large
// enough (or nil) a new array will be allocated and returned.
func (bf *BlockFile) Read(index BlockIndex, buf []byte) ([]byte, error) {
	return bf.ReadAt(index, 0, bf.Cache.BlockSize, buf)
}

// Read part of a block into the passed buffer. If the buffer is not large
// enough (or nil) a new array will be allocated and returned.
func (bf *BlockFile) ReadAt(index BlockIndex, off, sz int, buf []byte) ([]byte, error) {
	if sz < 0 || sz > bf.Cache.BlockSize {
		return nil, errors.New("invalid block size")
	} else if off < 0 || off > bf.Cache.BlockSize-sz {
		return nil, errors.New("read outside of block")
	}
	if sz <= cap(buf) {
		buf = buf[:sz]
	} else {
		buf = make([]byte, sz)
	}
	err := bf.Cache.Access(bf, index, true, func(tag interface{}, data []byte, found bool) (interface{}, bool, error) {
		if !found {
			err := readAtFull(bf.File, index*int64(bf.Cache.BlockSize), data)
			if err != nil && err != io.EOF {
				return tag, false, err
			}
		}
		copy(buf, data[off:])
		return tag, false, nil
	})
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// Write an entire block to the block file. buf must be exactly bf.BlockSize in
// length.
func (bf *BlockFile) Write(tag interface{}, index BlockIndex, buf []byte) error {
	if len(buf) != bf.Cache.BlockSize {
		return errors.New("write must cover entire block")
	}
	return bf.WriteAt(tag, index, 0, buf)
}

// Write part of a block to the block file.
func (bf *BlockFile) WriteAt(tag interface{}, index BlockIndex, off int, buf []byte) error {
	if len(buf) > bf.Cache.BlockSize {
		return errors.New("write data too large")
	} else if off < 0 || off > bf.Cache.BlockSize-len(buf) {
		return errors.New("write outside of block")
	}

	return bf.Cache.Access(bf, index, true, func(prevTag interface{}, data []byte, found bool) (interface{}, bool, error) {
		if !found && len(buf) < bf.Cache.BlockSize {
			err := readAtFull(bf.File, index*int64(bf.Cache.BlockSize), data)
			if err != nil {
				return prevTag, false, err
			}
		}
		copy(data[off:], buf)
		bf.updateCacheTag(index, prevTag, tag)
		return tag, true, nil
	})
}

func (bf *BlockFile) zeroBlock(tag interface{}, index BlockIndex) error {
	return bf.Cache.Access(bf, index, true, func(prevTag interface{}, data []byte, found bool) (interface{}, bool, error) {
		for i := 0; i < len(data); i++ {
			data[i] = 0
		}
		bf.updateCacheTag(index, prevTag, tag)
		return tag, true, nil
	})
}

func (bf *BlockFile) updateCacheTag(index BlockIndex, oldTag interface{}, newTag interface{}) {
	if oldTag == newTag {
		return
	}
	bf.tagLock.Lock()
	defer bf.tagLock.Unlock()

	if oldTag != nil {
		tagBlocks := bf.tagDirtyBlocks[oldTag]
		delete(tagBlocks, index)
		if len(tagBlocks) == 0 {
			delete(bf.tagDirtyBlocks, oldTag)
		}
	}

	if newTag != nil {
		tagBlocks, ok := bf.tagDirtyBlocks[newTag]
		if !ok {
			tagBlocks = make(map[BlockIndex]struct{})
			bf.tagDirtyBlocks[newTag] = tagBlocks
		}
		tagBlocks[index] = struct{}{}
	}
}

func (bf *BlockFile) SyncTag(tag interface{}) error {
	bf.tagLock.Lock()

	var blocks []BlockIndex
	tagBlocks, ok := bf.tagDirtyBlocks[tag]
	if ok {
		blocks = make([]BlockIndex, 0, len(tagBlocks))
		for block := range tagBlocks {
			blocks = append(blocks, block)
		}
	}
	bf.tagLock.Unlock()

	for _, block := range blocks {
		err := bf.Cache.Flush(bf, block)
		if err != nil {
			return err
		}
	}

	if tag == bf {
		return bf.File.Sync()
	}
	return bf.SyncTag(bf)
}

func (bf *BlockFile) AccessBlockMeta(index BlockIndex, accessFunc func(meta []byte) (bool, error)) error {
	if index <= 0 {
		panic("no metadata for index")
	}

	metaIndex := (index + int64(bf.blocksPerMeta - 1)) / int64(bf.blocksPerMeta) * int64(bf.blocksPerMeta)
	return bf.Cache.Access(bf, metaIndex, true, func(_ interface{}, data []byte, found bool) (interface{}, bool, error) {
		if !found {
			err := readAtFull(bf.File, metaIndex*int64(bf.Cache.BlockSize), data)
			if err != nil {
				return bf, false, err
			}
		}
		metaOffset := bf.MetaDataSize * int(index % int64(bf.blocksPerMeta))
		updated, err := accessFunc(data[metaOffset:metaOffset+bf.MetaDataSize])
		if err != nil {
			return bf, false, err
		}
		return bf, updated, nil
	})
}

func (bf *BlockFile) IsBlockReadOnly(index BlockIndex) bool {
	return false
}
