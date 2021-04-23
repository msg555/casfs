package storage

import (
	"errors"

	"github.com/msg555/ctrfs/blockfile"
)

const (
	HASH_HEADER_DATA_BLOCK = "data:"
	HASH_HEADER_FILE_BLOCK = "file:"
	HASH_HEADER_DIR_BLOCK = "dir:"
	HASH_HEADER_FILE_TREE_BLOCK = "filetree:"
	HASH_HEADER_DIR_TREE_BLOCK = "filetree:"
	HASH_HEADER_HARDLINK_BLOCK = "hardlink:"
)

type blockObject interface {
	cacheContentAddress(sc *StorageContext) ([]byte, error)
}

/*
func getBlockContentAddress(bf blockfile.BlockAllocator, blockIndex blockfile.BlockIndex) ([]byte, error) {
	return nil, nil
}
*/

func (sc *StorageContext) insertBlockIntoCache(contentAddress []byte, blockIndex InodeId) error {
	var val [8]byte
	bo.PutUint64(val[:], uint64(blockIndex))
	return sc.dataBlockCache.Insert(sc, DATA_BLOCK_CACHE_NODE, contentAddress, val[:], false)
}

func (sc *StorageContext) lookupAddressInode(contentAddress []byte) (InodeId, error) {
	val, _, err := sc.dataBlockCache.Find(DATA_BLOCK_CACHE_NODE, contentAddress)
	if err != nil {
		return 0, err
	}
	return blockfile.BlockIndex(bo.Uint64(val)), nil
}

func (sc *StorageContext) cacheDataBlockContentAddress(blockIndex InodeId) ([]byte, error) {
	hsh := sc.HashFactory()
	hsh.Write([]byte(HASH_HEADER_DATA_BLOCK))
	err := sc.Blocks.AccessBlock(nil, blockIndex, func(data []byte) (bool, error) {
		hsh.Write(data)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	h := hsh.Sum(nil)
	err = sc.Blocks.AccessBlockMeta(blockIndex, func(meta []byte) (bool, error) {
		if len(meta) < len(h) {
			return false, errors.New("metadata too small to store content address")
		}
		copy(meta, h)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return h, nil
}


/*
TODO
func updateBlockRefCount(bf blockfile.BlockAllocator, blockIndex blockfile.BlockIndex, ref int) (bool, error) {
	return false, nil
}
*/
