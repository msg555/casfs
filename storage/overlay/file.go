package overlay

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/blockcache"
	"github.com/msg555/ctrfs/storage"
	"github.com/msg555/ctrfs/unix"
)

var bo = binary.LittleEndian

const HEADER_SIZE = 9

type fileOverlay struct {
	Src   *os.File
	Dst   *os.File
	Cache *blockcache.BlockCache

	// Current live inode data for the file.
	InodeData storage.InodeData

	// Last inode data synced to the dst file.
	dstInodeData storage.InodeData

	// inode data from the source file.
	srcInodeData storage.InodeData

	// Minimum size the file has ever been.
	minFileSize uint64
}

func OpenFileOverlay(srcPath, dstPath string, srcInodeData *storage.InodeData, dstPerm os.FileMode, cache *blockcache.BlockCache) (*fileOverlay, error) {
	if cache.BlockSize < HEADER_SIZE+storage.INODE_SIZE {
		return nil, errors.New("blocksize too small")
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return nil, err
	}
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR, dstPerm)
	if err != nil {
		src.Close()
		return nil, err
	}

	fo := &fileOverlay{
		Src:          src,
		Dst:          dst,
		InodeData:    *srcInodeData,
		dstInodeData: *srcInodeData,
		srcInodeData: *srcInodeData,
		minFileSize:  srcInodeData.Size,
		Cache:        cache,
	}

	err = fo.init()
	if err != nil {
		fo.Close()
		return nil, err
	}

	return fo, nil
}

func (f *fileOverlay) init() error {
	buf := make([]byte, HEADER_SIZE+storage.INODE_SIZE)

	err := f.readBlock(false, 0, 0, buf)
	if err != nil {
		return nil
	}

	// Check if the header block has been initialized.
	if buf[0] == 0 {
		// If not then we just leave the inode data as it was initialized.
		return nil
	}

	f.minFileSize = bo.Uint64(buf[1:])
	f.InodeData = *storage.InodeFromBytes(buf[9:])
	f.dstInodeData = f.InodeData

	return nil
}

func (f *fileOverlay) Sync() error {
	err := f.Cache.Access(f, int64(0), true, func(buf []byte, found bool) (bool, error) {
		buf[0] = 1
		bo.PutUint64(buf[1:], f.minFileSize)
		copy(buf[9:], f.InodeData.ToBytes())
		return true, nil
	})
	if err != nil {
		return err
	}

	err = f.Cache.FlushGroup(f)
	if err != nil {
		return err
	}

	return f.Dst.Sync()
}

func (f *fileOverlay) Close() error {
	errSync := f.Sync()
	if errSync != nil {
		return errSync
	}
	errSrc := f.Src.Close()
	errDst := f.Dst.Close()
	if errSrc != nil {
		return errSrc
	}
	f.Cache.RemoveGroup(f)
	return errDst
}

// Flush a single block back to disk. This is called through by the BlockCache
// and implements the blockcache.Flushable interface.
func (f *fileOverlay) FlushBlock(ikey interface{}, buf []byte) error {
	key := ikey.(int64)
	if key%2 == 1 {
		panic("unexpectedly flushing src block")
	}
	block := key / 2

	written := 0
	offset := block * int64(f.Cache.BlockSize)
	for written < len(buf) {
		n, err := f.Dst.WriteAt(buf[written:], offset+int64(written))
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

/* Read block data into 'data'. offset is a relative offset within the block. */
func (f *fileOverlay) readBlock(fromSrc bool, block, offset int64, data []byte) error {
	if offset < 0 || int64(f.Cache.BlockSize) <= offset {
		panic("invalid read offset")
	}

	file := f.Dst
	fileInd := int64(0)
	if fromSrc {
		file = f.Src
		fileInd = 1
	}

	return f.Cache.Access(f, block*2+fileInd, true, func(buf []byte, found bool) (bool, error) {
		if found {
			copy(data, buf[offset:])
			return false, nil
		}

		read := 0
		fileOffset := block * int64(f.Cache.BlockSize)
		for read < len(buf) {
			n, err := file.ReadAt(buf[read:], fileOffset+int64(read))
			if err == io.EOF {
				break
			}
			if err != nil {
				return false, err
			}
			read += n
		}

		copy(data, buf[offset:])
		return false, nil
	})
}

/* Write data into a block. */
func (f *fileOverlay) writeBlock(block, offset int64, data []byte) error {
	if offset < 0 || int64(f.Cache.BlockSize) <= offset {
		panic("invalid read offset")
	}
	if len(data) == 0 {
		return nil
	}

	return f.Cache.Access(f, block*2, true, func(buf []byte, found bool) (bool, error) {
		// Read original block if our write doesn't completely cover it.
		if !found && (offset != 0 || len(buf) < f.Cache.BlockSize) {
			read := 0
			offset := block * int64(f.Cache.BlockSize)
			for read < len(buf) {
				n, err := f.Dst.ReadAt(buf[read:], offset+int64(read))
				if err == io.EOF {
					break
				}
				if err != nil {
					return false, err
				}
				read += n
			}
		}

		copy(buf[offset:], data)
		return true, nil
	})
}

// Given a range of data block indexes starting at dataBlockStart and going
// until dataBlockStart+count-1, return an array of booleans to indicate if each
// of them is dirty.
func (f *fileOverlay) isDirty(dataBlockStart int64, count int) ([]bool, error) {
	var result []bool

	lastBlock := int64(-1)
	buf := make([]byte, f.Cache.BlockSize)

	for i := dataBlockStart; i < dataBlockStart+int64(count); i++ {
		metaBlockIndex := i / int64(f.Cache.BlockSize*8)
		metaBlockPos := i % int64(f.Cache.BlockSize*8)
		block := 1 + metaBlockIndex*int64(f.Cache.BlockSize+1)

		if block != lastBlock {
			err := f.readBlock(false, block, 0, buf)
			if err != nil {
				return nil, err
			}
			lastBlock = block
		}

		result = append(result, (buf[metaBlockPos>>3]&(1<<(metaBlockPos&7))) != 0)
	}

	return result, nil
}

// Set every block in the range starting from dataBlockStart to
// dataBlockStart+count-1 to dirty.
func (f *fileOverlay) markDirty(dataBlockStart int64, count int) error {
	if count <= 0 {
		return nil
	}

	lastBlock := int64(-1)
	buf := make([]byte, f.Cache.BlockSize)

	for i := dataBlockStart; i < dataBlockStart+int64(count); i++ {
		metaBlockIndex := i / int64(f.Cache.BlockSize*8)
		metaBlockPos := i % int64(f.Cache.BlockSize*8)
		block := 1 + metaBlockIndex*int64(f.Cache.BlockSize+1)

		if block != lastBlock {
			if lastBlock != -1 {
				err := f.writeBlock(lastBlock, 0, buf)
				if err != nil {
					return err
				}
			}
			err := f.readBlock(false, block, 0, buf)
			if err != nil {
				return err
			}
			lastBlock = block
		}

		buf[metaBlockPos>>3] |= 1 << (metaBlockPos & 7)
	}

	return f.writeBlock(lastBlock, 0, buf)
}

func (f *fileOverlay) dataBlockIndex(dataBlock int64) int64 {
	return 2 + dataBlock + dataBlock/int64(8*f.Cache.BlockSize)
}

func (f *fileOverlay) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, unix.EINVAL
	}
	if off >= int64(f.InodeData.Size) {
		return 0, nil
	}

	if int64(len(p)) > int64(f.InodeData.Size)-off {
		p = p[:int64(f.InodeData.Size)-int64(off)]
	}

	dataBlockStart := off / int64(f.Cache.BlockSize)
	dataBlockEnd := (off + int64(len(p)-1)) / int64(f.Cache.BlockSize)
	dirty, err := f.isDirty(dataBlockStart, int(dataBlockEnd-dataBlockStart+1))
	if err != nil {
		return 0, err
	}

	read := 0
	for block := dataBlockStart; read < len(p); block++ {
		startInd := off - int64(block)*int64(f.Cache.BlockSize)
		if startInd < 0 {
			startInd = 0
		}

		endInd := off + int64(len(p)) - int64(block)*int64(f.Cache.BlockSize)
		if int64(f.Cache.BlockSize) < endInd {
			endInd = int64(f.Cache.BlockSize)
		}

		if dirty[block-dataBlockStart] {
			err := f.readBlock(false, f.dataBlockIndex(block), startInd, p[read:])
			if err != nil {
				return 0, err
			}
		} else if block*int64(f.Cache.BlockSize) < int64(f.minFileSize) {
			err := f.readBlock(true, block, startInd, p[read:])
			if err != nil {
				return 0, err
			}

			// Zero any bytes read that are beyond minFileSize
			for i := int64(f.minFileSize) - block*int64(f.Cache.BlockSize) - startInd + int64(read); i < int64(read)+endInd-startInd; i++ {
				p[i] = 0
			}
		} else {
			// For non-dirty blocks passed the end of our source block we just give
			// zeroes.
			for i := read; i < read+int(endInd-startInd); i++ {
				p[i] = 0
			}
		}

		read += int(endInd - startInd)
	}

	return read, nil
}

func (f *fileOverlay) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, unix.EINVAL
	}
	if len(p) == 0 {
		// From my understanding of write(2) if zero bytes are written it will not
		// extend the file even if the file pointer is beyond the end of the file.
		return 0, nil
	}

	dataBlockStart := off / int64(f.Cache.BlockSize)
	dataBlockEnd := (off + int64(len(p)-1)) / int64(f.Cache.BlockSize)
	dirty, err := f.isDirty(dataBlockStart, int(dataBlockEnd-dataBlockStart+1))
	if err != nil {
		return 0, err
	}

	written := 0
	for block := dataBlockStart; written < len(p); block++ {
		startInd := off - int64(block)*int64(f.Cache.BlockSize)
		if startInd < 0 {
			startInd = 0
		}

		endInd := off + int64(len(p)) - int64(block)*int64(f.Cache.BlockSize)
		if int64(f.Cache.BlockSize) < endInd {
			endInd = int64(f.Cache.BlockSize)
		}

		if dirty[block-dataBlockStart] || (startInd == 0 && endInd == int64(f.Cache.BlockSize)) {
			// Easy case - block is already dirty or covers an entire block.
			err := f.writeBlock(f.dataBlockIndex(block), 0, p[written:])
			if err != nil {
				return 0, err
			}
		} else {
			// Otherwise need to read from src, add our write on top of it, and write
			// that to dst.
			buf := make([]byte, f.Cache.BlockSize)

			err := f.readBlock(true, block, 0, buf)
			if err != nil {
				return 0, err
			}

			copy(buf[startInd:], p[written:])

			err = f.writeBlock(f.dataBlockIndex(block), 0, buf)
			if err != nil {
				return 0, err
			}
		}

		written += int(endInd - startInd)
	}

	err = f.markDirty(dataBlockStart, int(dataBlockEnd-dataBlockStart+1))
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (f *fileOverlay) Truncate(length int64) error {
	if length < 0 {
		return unix.EINVAL
	}

	if uint64(length) < f.minFileSize {
		f.minFileSize = uint64(length)
	}
	f.InodeData.Size = uint64(length)

	if length == 0 {
		return f.Dst.Truncate(int64(f.Cache.BlockSize))
	}

	block := f.dataBlockIndex((length - 1) / int64(f.Cache.BlockSize))
	return f.Dst.Truncate(block*int64(f.Cache.BlockSize) + (length-1)%int64(f.Cache.BlockSize) + 1)
}
