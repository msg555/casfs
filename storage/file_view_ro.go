package storage

import (
	"io"
	"os"

	"github.com/msg555/ctrfs/blockcache"
	"github.com/msg555/ctrfs/unix"
)

// Implements storage.FileView
type roFileOverlay struct {
	file      *os.File
	inodeData InodeData
	cache     *blockcache.BlockCache
	groupKey  interface{}
}

func OpenROFileOverlayFromFile(file *os.File, inodeData *InodeData, cache *blockcache.BlockCache, groupKey interface{}) *roFileOverlay {
	return &roFileOverlay{
		file:      file,
		inodeData: *inodeData,
		cache:     cache,
		groupKey:  groupKey,
	}
}

func OpenROFileOverlay(path string, inodeData *InodeData, cache *blockcache.BlockCache, groupKey interface{}) (*roFileOverlay, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return OpenROFileOverlayFromFile(f, inodeData, cache, groupKey), nil
}

func (f *roFileOverlay) Close() error {
	// Leave blocks in cache to be re-used or evicted later.
	return f.file.Close()
}

func (f *roFileOverlay) GetInode() InodeData {
	return f.inodeData
}

func (f *roFileOverlay) UpdateInode(accessFunc func(*InodeData) error) error {
	return unix.EROFS
}

func (f *roFileOverlay) Sync() error {
	// Nothing to sync
	return nil
}

func (f *roFileOverlay) ReadAt(p []byte, off int64) (int, error) {
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

		err := f.cache.Access(f.groupKey, block, true, func(buf []byte, found bool) (bool, error) {
			if !found {
				blockDataRead := 0
				fileOffset := block * int64(f.cache.BlockSize)
				for blockDataRead < len(buf) {
					n, err := f.file.ReadAt(buf[blockDataRead:], fileOffset+int64(blockDataRead))
					if err == io.EOF {
						break
					}
					if err != nil {
						return false, err
					}
					blockDataRead += n
				}
			}

			copy(p[read:], buf[startInd:])
			return false, nil
		})
		if err != nil {
			return 0, err
		}

		read += int(endInd - startInd)
	}

	return read, nil
}

func (f *roFileOverlay) WriteAt([]byte, int64) (int, error) {
	return 0, unix.EROFS
}
