package storage

import (
	"io"
	"os"

	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/blockcache"
	"github.com/msg555/ctrfs/unix"
)

const HEADER_SIZE = 9

type fileOverlay struct {
	src   FileView
	file  *os.File
	cache *blockcache.BlockCache

	// Current live inode data for the file.
	inodeData InodeData

	// Minimum size the file has ever been.
	minFileSize uint64
}

func OpenFileOverlay(src FileView, dstPath string, dstPerm os.FileMode, cache *blockcache.BlockCache) (*fileOverlay, error) {
	if cache.BlockSize < HEADER_SIZE+INODE_SIZE {
		return nil, errors.New("blocksize too small")
	}

	file, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR, dstPerm)
	if err != nil {
		return nil, err
	}

	inodeData := src.GetInode()
	f := &fileOverlay{
		src:         src,
		file:        file,
		cache:       cache,
		inodeData:   inodeData,
		minFileSize: inodeData.Size,
	}

	buf := f.cache.Pool.Get().([]byte)
	defer f.cache.Pool.Put(buf)

	err = f.readBlock(0, 0, buf)
	if err != nil {
		file.Close()
		return nil, err
	}

	if buf[0] == 1 {
		f.minFileSize = bo.Uint64(buf[1:])
		f.inodeData = *InodeFromBytes(buf[9:])
	}

	return f, nil
}

func (f *fileOverlay) GetInode() InodeData {
	return f.inodeData
}

func (f *fileOverlay) UpdateInode(accessFunc func(*InodeData) error) error {
	origSize := f.inodeData.Size
	err := accessFunc(&f.inodeData)
	if err != nil {
		return err
	}

	if f.inodeData.Size < origSize {
		// Shrinking the file size requires the cache to be flushed currently. We
		// could theoretically optimize this and have unneeded pages simply
		// discarded and the page bordering the update updated. This would avoid
		// having any disk I/O except the call to truncate(2) itself.
		err := f.Sync()
		if err != nil {
			return err
		}

		if f.inodeData.Size < f.minFileSize {
			f.minFileSize = f.inodeData.Size
		}

		block := f.dataBlockIndex(int64(f.inodeData.Size-1) / int64(f.cache.BlockSize))
		err = f.file.Truncate(block*int64(f.cache.BlockSize) + int64(f.inodeData.Size-1)%int64(f.cache.BlockSize) + 1)
		if err != nil {
			return err
		}
	}

	return f.cache.Access(f, int64(0), true, func(buf []byte, found bool) (bool, error) {
		buf[0] = 1
		bo.PutUint64(buf[1:], f.minFileSize)
		copy(buf[9:], f.inodeData.ToBytes())
		return true, nil
	})
}

func (f *fileOverlay) Sync() error {
	err := f.cache.FlushGroup(f)
	if err != nil {
		return err
	}
	return f.file.Sync()
}

func (f *fileOverlay) Close() error {
	errRem := f.cache.RemoveGroup(f)
	errClose := f.file.Close()
	if errRem != nil {
		return errRem
	}
	return errClose
}

// Flush a single block back to disk. This is called through by the BlockCache
// and implements the blockcache.Flushable interface.
func (f *fileOverlay) FlushBlock(ikey interface{}, buf []byte) error {
	block := ikey.(int64)

	written := 0
	offset := block * int64(f.cache.BlockSize)
	for written < len(buf) {
		n, err := f.file.WriteAt(buf[written:], offset+int64(written))
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

/* Read block data into 'data'. offset is a relative offset within the block. */
func (f *fileOverlay) readBlock(block, offset int64, data []byte) error {
	if offset < 0 || int64(f.cache.BlockSize) <= offset {
		panic("invalid read offset")
	}

	return f.cache.Access(f, block, true, func(buf []byte, found bool) (bool, error) {
		if found {
			copy(data, buf[offset:])
			return false, nil
		}

		read := 0
		fileOffset := block * int64(f.cache.BlockSize)
		for read < len(buf) {
			n, err := f.file.ReadAt(buf[read:], fileOffset+int64(read))
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
	if offset < 0 || int64(f.cache.BlockSize) <= offset {
		panic("invalid write offset")
	}
	if len(data) == 0 {
		return nil
	}

	return f.cache.Access(f, block, true, func(buf []byte, found bool) (bool, error) {
		// Read original block if our write doesn't completely cover it.
		if !found && (offset != 0 || len(buf) < f.cache.BlockSize) {
			read := 0
			offset := block * int64(f.cache.BlockSize)
			for read < len(buf) {
				n, err := f.file.ReadAt(buf[read:], offset+int64(read))
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
	buf := make([]byte, f.cache.BlockSize)

	for i := dataBlockStart; i < dataBlockStart+int64(count); i++ {
		metaBlockIndex := i / int64(f.cache.BlockSize*8)
		metaBlockPos := i % int64(f.cache.BlockSize*8)
		block := 1 + metaBlockIndex*int64(f.cache.BlockSize+1)

		if block != lastBlock {
			err := f.readBlock(block, 0, buf)
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
	buf := make([]byte, f.cache.BlockSize)

	for i := dataBlockStart; i < dataBlockStart+int64(count); i++ {
		metaBlockIndex := i / int64(f.cache.BlockSize*8)
		metaBlockPos := i % int64(f.cache.BlockSize*8)
		block := 1 + metaBlockIndex*int64(f.cache.BlockSize+1)

		if block != lastBlock {
			if lastBlock != -1 {
				err := f.writeBlock(lastBlock, 0, buf)
				if err != nil {
					return err
				}
			}
			err := f.readBlock(block, 0, buf)
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
	return 2 + dataBlock + dataBlock/int64(8*f.cache.BlockSize)
}

func (f *fileOverlay) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, unix.EINVAL
	}
	if off >= int64(f.inodeData.Size) {
		return 0, nil
	}

	if int64(len(p)) > int64(f.inodeData.Size)-off {
		p = p[:int64(f.inodeData.Size)-int64(off)]
	}

	dataBlockStart := off / int64(f.cache.BlockSize)
	dataBlockEnd := (off + int64(len(p)-1)) / int64(f.cache.BlockSize)
	dirty, err := f.isDirty(dataBlockStart, int(dataBlockEnd-dataBlockStart+1))
	if err != nil {
		return 0, err
	}

	read := 0
	for block := dataBlockStart; read < len(p); block++ {
		startInd := off - int64(block)*int64(f.cache.BlockSize)
		if startInd < 0 {
			startInd = 0
		}

		endInd := off + int64(len(p)) - int64(block)*int64(f.cache.BlockSize)
		if int64(f.cache.BlockSize) < endInd {
			endInd = int64(f.cache.BlockSize)
		}
		pread := p[read : read+int(endInd-startInd)]

		if dirty[block-dataBlockStart] {
			err := f.readBlock(f.dataBlockIndex(block), startInd, pread)
			if err != nil {
				return 0, err
			}
		} else if block*int64(f.cache.BlockSize) < int64(f.minFileSize) {
			_, err := f.src.ReadAt(pread, block*int64(f.cache.BlockSize)+startInd)
			if err != nil {
				return 0, err
			}

			// Zero any bytes read that are beyond minFileSize
			for i := int64(f.minFileSize) - block*int64(f.cache.BlockSize) - startInd + int64(read); i < int64(read)+endInd-startInd; i++ {
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

	dataBlockStart := off / int64(f.cache.BlockSize)
	dataBlockEnd := (off + int64(len(p)-1)) / int64(f.cache.BlockSize)
	dirty, err := f.isDirty(dataBlockStart, int(dataBlockEnd-dataBlockStart+1))
	if err != nil {
		return 0, err
	}

	written := 0
	var tmpBuf []byte
	defer func() {
		if tmpBuf != nil {
			f.cache.Pool.Put(tmpBuf)
		}
	}()
	for block := dataBlockStart; written < len(p); block++ {
		startInd := off - int64(block)*int64(f.cache.BlockSize)
		if startInd < 0 {
			startInd = 0
		}

		endInd := off + int64(len(p)) - int64(block)*int64(f.cache.BlockSize)
		if int64(f.cache.BlockSize) < endInd {
			endInd = int64(f.cache.BlockSize)
		}

		if dirty[block-dataBlockStart] || (startInd == 0 && endInd == int64(f.cache.BlockSize)) {
			// Easy case - block is already dirty or covers an entire block.
			err := f.writeBlock(f.dataBlockIndex(block), 0, p[written:])
			if err != nil {
				return 0, err
			}
		} else {
			// Otherwise need to read from src, add our write on top of it, and write
			// that to dst.
			if tmpBuf == nil {
				tmpBuf = f.cache.Pool.Get().([]byte)
			}

			var n int
			if block*int64(f.cache.BlockSize) < int64(f.minFileSize) {
				n, err = f.src.ReadAt(tmpBuf, block*int64(f.cache.BlockSize))
				if err != nil {
					return 0, err
				}
				maxN := int64(f.minFileSize) - block*int64(f.cache.BlockSize)
				if int64(n) > maxN {
					n = int(maxN)
				}
			}
			for i := n; i < len(tmpBuf); i++ {
				tmpBuf[i] = 0
			}

			copy(tmpBuf[startInd:], p[written:])

			err = f.writeBlock(f.dataBlockIndex(block), 0, tmpBuf)
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

	if uint64(off)+uint64(len(p)) > f.inodeData.Size {
		f.inodeData.Size = uint64(off) + uint64(len(p))
	}

	return len(p), nil
}
