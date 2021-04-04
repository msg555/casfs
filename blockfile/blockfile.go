package blockfile

import (
	"encoding/binary"
	"os"

	"github.com/go-errors/errors"
)

var bo = binary.LittleEndian

type BlockIndex = uint64

// Manages access to a blockfile, allowing blocks to be allocated, deleted,
// read, and written to. The Allocate() and Delete() methods are not thread safe
// and it is the responsibility of the caller to ensure that multiple
// threads/processes are not calling these methods at the same time. Note that
// the blockfile does not ever shrink and has size proportional to the largest
// number of blocks that were simultaneously allocated.
type BlockFile struct {
	// The size in bytes of each block. This is the number of bytes that will be
	// read or should be written. Note that internally there is a small amount of
	// overhead on top of this per block.
	BlockSize int

	file              *os.File
	internalBlockSize int
	blocks            BlockIndex
	freeListHead      BlockIndex
}

func readAtFull(file *os.File, offset uint64, size int, buf []byte) ([]byte, error) {
	if size <= cap(buf) {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	read := 0
	for read < size {
		n, err := file.ReadAt(buf[read:], int64(offset)+int64(read))
		if err != nil {
			return nil, err
		}
		read += n
	}

	return buf, nil
}

func writeAtFull(file *os.File, offset uint64, buf []byte) error {
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

// Initializes the blockfile based on the passed file pointer. This file handle
// will be closed when bf.Close() is later invoked and show not be closed by
// the caller.
func (bf *BlockFile) OpenFile(file *os.File) error {
	bf.file = file
	return bf.init()
}

// Opens the passed blockfile, creating it if necessary using `perm`
// permissions. If readOnly is set perm is ignored and the open will
// fail if the file does not exist.
func (bf *BlockFile) Open(path string, perm os.FileMode, readOnly bool) error {
	flags := os.O_CREATE | os.O_RDWR
	if readOnly {
		flags = os.O_RDONLY
	}
	file, err := os.OpenFile(path, flags, perm)
	if err != nil {
		return err
	}

	return bf.OpenFile(file)
}

// Closes the underlying file handle
func (bf *BlockFile) Close() error {
	return bf.file.Close()
}

func (bf *BlockFile) init() error {
	bf.internalBlockSize = bf.BlockSize + 8

	pos, err := bf.file.Seek(0, 2)
	if err != nil {
		return err
	}
	if pos%int64(bf.internalBlockSize) != 0 {
		return errors.New("unexpected file length")
	}

	bf.blocks = BlockIndex(pos) / BlockIndex(bf.internalBlockSize)
	if 0 < bf.blocks {
		data, err := readAtFull(bf.file, 0, 8, nil)
		if err != nil {
			return err
		}
		bf.freeListHead = bo.Uint64(data)
	} else {
		bf.Allocate()
	}

	return nil
}

// Allocate a new block and returns the index. The index is a positive uint64
// that will never exceed the maximum number of simultaneously allocated blocks.
func (bf *BlockFile) Allocate() (BlockIndex, error) {
	if bf.freeListHead == 0 {
		err := bf.file.Truncate(int64(bf.internalBlockSize) * int64(bf.blocks+1))
		if err != nil {
			return 0, err
		}

		bf.blocks++
		return bf.blocks - 1, nil
	}

	index := bf.freeListHead

	data, err := readAtFull(bf.file, uint64(bf.freeListHead)*uint64(bf.internalBlockSize), 8, nil)
	if err != nil {
		return 0, err
	}
	bf.freeListHead = bo.Uint64(data)
	err = writeAtFull(bf.file, 0, data)
	if err != nil {
		return 0, err
	}

	return index, nil
}

// Free a block and allow it to be allocated in the future.
func (bf *BlockFile) Free(index BlockIndex) error {
	var buf [8]byte

	bo.PutUint64(buf[:], bf.freeListHead)
	err := writeAtFull(bf.file, uint64(index)*uint64(bf.internalBlockSize), buf[:])
	if err != nil {
		return err
	}

	bo.PutUint64(buf[:], index)
	err = writeAtFull(bf.file, 0, buf[:])

	bf.freeListHead = index

	return nil
}

// Read an entire block into the passed buffer. If the buffer is not large
// enough (or nil) a new array will be allocated and returned.
func (bf *BlockFile) Read(index BlockIndex, buf []byte) ([]byte, error) {
	return readAtFull(bf.file, uint64(index)*uint64(bf.internalBlockSize)+8, int(bf.BlockSize), buf)
}

// Read part of a block into the passed buffer. If the buffer is not large
// enough (or nil) a new array will be allocated and returned.
func (bf *BlockFile) ReadAt(index BlockIndex, off, sz int, buf []byte) ([]byte, error) {
	if sz < 0 || sz > bf.BlockSize {
		return nil, errors.New("invalid block size")
	} else if off < 0 || off > bf.BlockSize-sz {
		return nil, errors.New("read outside of block")
	}
	return readAtFull(bf.file, uint64(index)*uint64(bf.internalBlockSize)+uint64(8+off), sz, buf)
}

// Write an entire block to the block file. buf must be exactly bf.BlockSize in
// length.
func (bf *BlockFile) Write(index BlockIndex, buf []byte) error {
	if len(buf) != int(bf.BlockSize) {
		return errors.New("can only make writes of size BlockSize")
	}
	return writeAtFull(bf.file, uint64(index)*uint64(bf.internalBlockSize)+8, buf)
}

// Write part of a block to the block file.
func (bf *BlockFile) WriteAt(index BlockIndex, off int, buf []byte) error {
	if len(buf) > bf.BlockSize {
		return errors.New("write data too large")
	} else if off < 0 || off > bf.BlockSize-len(buf) {
		return errors.New("write outside of block")
	}
	return writeAtFull(bf.file, uint64(index)*uint64(bf.internalBlockSize)+uint64(8+off), buf)
}
