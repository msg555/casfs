package blockfile

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/blockcache"
)

var bo = binary.LittleEndian

type BlockIndex = int64

type BlockFile struct {
	Cache     *blockcache.BlockCache
	File      *os.File
	AllocLock sync.Mutex
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
	return nil
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

// Allocate a new block and returns the index. The index is a positive int64
// that will never exceed the maximum number of simultaneously allocated blocks.
func (bf *BlockFile) Allocate() (BlockIndex, error) {
	bf.AllocLock.Lock()
	defer bf.AllocLock.Unlock()

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
			bo.PutUint64(dat[:], uint64(result))
		}
		err = bf.WriteAt(freeHead, (lo-1)*8, dat[:])
		if err != nil {
			return 0, err
		}
		err = bf.zeroBlock(result)
		if err != nil {
			return 0, err
		}
		return result, nil
	}

	// Otherwise the current free head is the new free block.
	err = bf.WriteAt(0, 0, buf[:8])
	if err != nil {
		return 0, err
	}

	err = bf.zeroBlock(freeHead)
	if err != nil {
		return 0, err
	}
	return freeHead, nil
}

// Free a block and allow it to be allocated in the future.
func (bf *BlockFile) Free(index BlockIndex) error {
	bf.AllocLock.Lock()
	defer bf.AllocLock.Unlock()

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
		return bf.WriteAt(freeHead, lo*8, dat[:])
	}

	// Make index the new free head.
	bo.PutUint64(buf, uint64(freeHead))
	for i := 8; i < len(buf); i++ {
		buf[i] = 0
	}
	err = bf.Write(index, buf)
	if err != nil {
		return err
	}

	bo.PutUint64(dat[:], uint64(index))
	return bf.WriteAt(0, 0, dat[:])
}

func (bf *BlockFile) FlushBlock(ikey interface{}, buf []byte) error {
	index := ikey.(BlockIndex)
	return writeAtFull(bf.File, index*int64(bf.Cache.BlockSize), buf)
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
	err := bf.Cache.Access(bf, index, true, func(data []byte, found bool) (bool, error) {
		if !found {
			err := readAtFull(bf.File, index*int64(bf.Cache.BlockSize), data)
			if err != nil && err != io.EOF {
				return false, err
			}
		}
		copy(buf, data[off:])
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// Write an entire block to the block file. buf must be exactly bf.BlockSize in
// length.
func (bf *BlockFile) Write(index BlockIndex, buf []byte) error {
	if len(buf) != bf.Cache.BlockSize {
		return errors.New("write must cover entire block")
	}
	return bf.WriteAt(index, 0, buf)
}

// Write part of a block to the block file.
func (bf *BlockFile) WriteAt(index BlockIndex, off int, buf []byte) error {
	if len(buf) > bf.Cache.BlockSize {
		return errors.New("write data too large")
	} else if off < 0 || off > bf.Cache.BlockSize-len(buf) {
		return errors.New("write outside of block")
	}

	return bf.Cache.Access(bf, index, true, func(data []byte, found bool) (bool, error) {
		if !found && len(buf) < bf.Cache.BlockSize {
			err := readAtFull(bf.File, index*int64(bf.Cache.BlockSize), data)
			if err != nil {
				return false, err
			}
		}
		copy(data[off:], buf)
		return true, nil
	})
}

func (bf *BlockFile) zeroBlock(index BlockIndex) error {
	return bf.Cache.Access(bf, index, true, func(data []byte, found bool) (bool, error) {
		for i := 0; i < len(data); i++ {
			data[i] = 0
		}
		return true, nil
	})
}
