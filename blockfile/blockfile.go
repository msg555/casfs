package blockfile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

var bo = binary.LittleEndian

type BlockIndex = uint64

type BlockFile struct {
	BlockSize int
	file      *os.File

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

func Open(path string, perm os.FileMode, blockSize int, readOnly bool) (*BlockFile, error) {
	flags := os.O_CREATE | os.O_RDWR
	if readOnly {
		flags = os.O_RDONLY
	}
	f, err := os.OpenFile(path, flags, perm)
	if err != nil {
		return nil, err
	}

	bf := &BlockFile{
		BlockSize: blockSize,
		file:      f,
	}
	bf.init()

	return bf, nil
}

// Closes the underlying file handle
func (bf *BlockFile) Close() error {
	return bf.file.Close()
}

func (bf *BlockFile) init() error {
	bf.internalBlockSize = bf.BlockSize + 8

	pos, err := bf.file.Seek(0, 2)
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

	fmt.Println("Got", pos, err)
	return nil
}

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

func (bf *BlockFile) Read(index BlockIndex, buf []byte) ([]byte, error) {
	return readAtFull(bf.file, uint64(index)*uint64(bf.internalBlockSize)+8, int(bf.BlockSize), buf)
}

func (bf *BlockFile) ReadAt(index BlockIndex, off, sz int, buf []byte) ([]byte, error) {
	if sz < 0 || sz > bf.BlockSize {
		return nil, errors.New("invalid block size")
	} else if off < 0 || off >= bf.BlockSize-sz {
		return nil, errors.New("read outside of block")
	}
	return readAtFull(bf.file, uint64(index)*uint64(bf.internalBlockSize)+uint64(8+off), sz, buf)
}

func (bf *BlockFile) Write(index BlockIndex, buf []byte) error {
	if len(buf) != int(bf.BlockSize) {
		return errors.New("can only make writes of size BlockSize")
	}
	return writeAtFull(bf.file, uint64(index)*uint64(bf.internalBlockSize)+8, buf)
}

func main() {
	bf, err := Open("block.file", 0666, 128, false)
	if err != nil {
		panic(err)
	}
	defer bf.Close()

	id, err := bf.Allocate()
	if err != nil {
		panic(err)
	}
	fmt.Println("Allocate", id)

	/*
		for i := BlockIndex(1); i <= id; i++ {
			err = bf.Free(BlockIndex(i))
		}
		if err != nil {
			panic(err)
		}
		fmt.Println("Free", id)
	*/
}
