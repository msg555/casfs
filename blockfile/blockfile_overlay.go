package blockfile

import (
	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/blockcache"
)

type BlockOverlayAllocator struct {
	blockSize    int
	metaDataSize int
	wrIndexShift BlockIndex

	roAllocator BlockAllocator
	wrAllocator BlockAllocator
}

func (bf *BlockOverlayAllocator) Init(roAllocator, wrAllocator BlockAllocator) error {
	wrIndexShift, err := roAllocator.GetNumBlocks()
	if err != nil {
		return err
	}

	bf.blockSize = roAllocator.GetBlockSize()
	bf.metaDataSize = roAllocator.GetMetaDataSize()
	bf.wrIndexShift = wrIndexShift
	bf.roAllocator = roAllocator
	bf.wrAllocator = wrAllocator
	if bf.blockSize != wrAllocator.GetBlockSize() {
		return errors.New("layers must have matching block size")
	}
	if bf.metaDataSize != wrAllocator.GetMetaDataSize() {
		return errors.New("layers must have matching meta data size")
	}
	return nil
}

func (bf *BlockOverlayAllocator) GetBlockSize() int {
	return bf.blockSize
}

func (bf *BlockOverlayAllocator) GetMetaDataSize() int {
	return bf.metaDataSize
}

func (bf *BlockOverlayAllocator) GetNumBlocks() (BlockIndex, error) {
	res, err := bf.wrAllocator.GetNumBlocks()
	if err != nil {
		return 0, err
	}
	return bf.wrIndexShift + res, nil
}

func (bf *BlockOverlayAllocator) GetCache() *blockcache.BlockCache {
	return bf.wrAllocator.GetCache()
}

func (bf *BlockOverlayAllocator) Close() error {
	return nil
}

func (bf *BlockOverlayAllocator) Allocate(tag interface{}) (BlockIndex, error) {
	index, err := bf.wrAllocator.Allocate(tag)
	if err != nil {
		return 0, err
	}
	return bf.wrIndexShift + index, nil
}

func (bf *BlockOverlayAllocator) Free(index BlockIndex) error {
	if index < bf.wrIndexShift {
		return errors.New("cannot free read only block")
	}
	return bf.wrAllocator.Free(index - bf.wrIndexShift)
}

func (bf *BlockOverlayAllocator) Read(index BlockIndex, buf []byte) ([]byte, error) {
	if index < bf.wrIndexShift {
		return bf.roAllocator.Read(index, buf)
	}
	return bf.wrAllocator.Read(index-bf.wrIndexShift, buf)
}

func (bf *BlockOverlayAllocator) ReadAt(index BlockIndex, off, sz int, buf []byte) ([]byte, error) {
	if index < bf.wrIndexShift {
		return bf.roAllocator.ReadAt(index, off, sz, buf)
	}
	return bf.wrAllocator.ReadAt(index-bf.wrIndexShift, off, sz, buf)
}

func (bf *BlockOverlayAllocator) Write(tag interface{}, index BlockIndex, buf []byte) error {
	if index < bf.wrIndexShift {
		return errors.New("cannot write to ro block")
	}
	return bf.wrAllocator.Write(tag, index-bf.wrIndexShift, buf)
}

func (bf *BlockOverlayAllocator) WriteAt(tag interface{}, index BlockIndex, off int, buf []byte) error {
	if index < bf.wrIndexShift {
		return errors.New("cannot write to ro block")
	}
	return bf.wrAllocator.WriteAt(tag, index-bf.wrIndexShift, off, buf)
}

func (bf *BlockOverlayAllocator) SyncTag(tag interface{}) error {
	return bf.wrAllocator.SyncTag(tag)
}

func (bf *BlockOverlayAllocator) AccessBlock(tag interface{}, index BlockIndex, accessFunc func(data []byte) (modified bool, err error)) error {
	if index < bf.wrIndexShift {
		return bf.roAllocator.AccessBlock(tag, index, func(meta []byte) (bool, error) {
			modified, err := accessFunc(meta)
			if err != nil {
				return false, err
			}
			if modified {
				return false, errors.New("cannot modify ro block")
			}
			return false, nil
		})
	}
	return bf.wrAllocator.AccessBlock(tag, index-bf.wrIndexShift, accessFunc)
}

func (bf *BlockOverlayAllocator) AccessBlockMeta(index BlockIndex, accessFunc func(meta []byte) (modified bool, err error)) error {
	if index < bf.wrIndexShift {
		return bf.roAllocator.AccessBlockMeta(index, func(meta []byte) (bool, error) {
			modified, err := accessFunc(meta)
			if err != nil {
				return false, err
			}
			if modified {
				return false, errors.New("cannot modify meta on ro block")
			}
			return false, nil
		})
	}
	return bf.wrAllocator.AccessBlockMeta(index-bf.wrIndexShift, accessFunc)
}

func (bf *BlockOverlayAllocator) IsBlockReadOnly(index BlockIndex) bool {
	return index < bf.wrIndexShift
}
