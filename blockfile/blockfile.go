package blockfile

import (
	"errors"
	"fmt"
	"os"

	"github.com/msg555/casfs"
)

type BlockIndex = uint64


type BlockFile struct {
	BlockSize					uint32
	File							*os.File

	internalBlockSize	uint32
	blocks						BlockIndex
	freeListHead			BlockIndex
}

func readAtFull(file *os.File, offset uint64, size int, buf []byte) ([]byte, error) {
	if size <= cap(buf) {
		buf = buf[:size]
	} else {
		buf = make([]byte, size)
	}

	read := 0
	for read < size {
		n, err := file.ReadAt(buf[read:], int64(offset) + int64(read))
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
		n, err := file.WriteAt(buf[written:], int64(offset) + int64(written))
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

func (bf *BlockFile) Init() error {
	bf.internalBlockSize = bf.BlockSize + 8

	pos, err := bf.File.Seek(0, 2)
	if pos % int64(bf.internalBlockSize) != 0 {
		return errors.New("unexpected file length")
	}

	bf.blocks = BlockIndex(pos) / BlockIndex(bf.internalBlockSize)
	if 0 < bf.blocks {
		data, err := readAtFull(bf.File, 0, 8, nil)
		if err != nil {
			return err
		}
		bf.freeListHead = casfs.Hbo.Uint64(data)
	} else {
		bf.Allocate()
	}

	fmt.Println("Got", pos, err)
	return nil
}

func (bf *BlockFile) Allocate() (BlockIndex, error) {
	if bf.freeListHead == 0 {
		err := bf.File.Truncate(int64(bf.internalBlockSize) * int64(bf.blocks + 1))
		if err != nil {
			return 0, err
		}

		bf.blocks++
		return bf.blocks - 1, nil
	}

	index := bf.freeListHead

	data, err := readAtFull(bf.File, uint64(bf.freeListHead) * uint64(bf.internalBlockSize), 8, nil)
	if err != nil {
		return 0, err
	}
	bf.freeListHead = casfs.Hbo.Uint64(data)
	err = writeAtFull(bf.File, 0, data)
	if err != nil {
		return 0, err
	}

	return index, nil
}

func (bf *BlockFile) Free(index BlockIndex) error {
	var buf [8]byte

	casfs.Hbo.PutUint64(buf[:], bf.freeListHead)
	err := writeAtFull(bf.File, uint64(index) * uint64(bf.internalBlockSize), buf[:])
	if err != nil {
		return err
	}

	casfs.Hbo.PutUint64(buf[:], index)
	err = writeAtFull(bf.File, 0, buf[:])

	bf.freeListHead = index

	return nil
}

func (bf *BlockFile) Read(index BlockIndex, buf []byte) ([]byte, error) {
	return readAtFull(bf.File, uint64(index) * uint64(bf.internalBlockSize) + 8, int(bf.BlockSize), buf)
}

func (bf *BlockFile) Write(index BlockIndex, buf []byte) error {
	if len(buf) != int(bf.BlockSize) {
		return errors.New("can only make writes of size BlockSize")
	}
	return writeAtFull(bf.File, uint64(index) * uint64(bf.internalBlockSize) + 8, buf)
}

func main() {
	f, err := os.OpenFile("block.file", os.O_CREATE | os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	bf := &BlockFile{
		BlockSize: 128,
		File: f,
	}

	err = bf.Init()
	if err != nil {
		panic(err)
	}

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
