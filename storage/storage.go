package storage

import (
	"crypto/sha256"
	"hash"
	"log"
	"os"
	"os/user"
	"path"

	"github.com/msg555/ctrfs/blockcache"
	"github.com/msg555/ctrfs/blockfile"
	"github.com/msg555/ctrfs/btree"
	"github.com/msg555/ctrfs/unix"
)

const (
	HASH_BYTE_LENGTH = 32

	DATA_BLOCK_CACHE_NODE = blockfile.BlockIndex(1)
)

type HashFactory func() hash.Hash

type StorageContext struct {
	HashFactory HashFactory
	Cache       *blockcache.BlockCache
	Blocks      blockfile.BlockAllocator
	FileManager TreeFileManager
	BasePath    string

	dataBlockCache	btree.BTree
}

type StorageNode struct {
	Inode       *InodeData
	NodeAddress [HASH_BYTE_LENGTH]byte
}

func OpenDefaultStorageContext() (*StorageContext, error) {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}

	return OpenStorageContext(path.Join(usr.HomeDir, ".ctrfs"))
}

func OpenStorageContext(basePath string) (*StorageContext, error) {
	hashFactory := sha256.New

	err := os.MkdirAll(path.Join(basePath, "mounts"), 0777)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	sc := &StorageContext{
		HashFactory: hashFactory,
		Cache: blockcache.New(65536, 4096),
		BasePath:    basePath,

		dataBlockCache: btree.BTree{
			MaxKeySize: HASH_BYTE_LENGTH,
			EntrySize: 8,
		},
	}

	bf := &blockfile.BlockFile{
		MetaDataSize: 36,
		Cache:        sc.Cache,
		PreAllocatedBlocks: 1,
	}
	err = bf.Open(path.Join(basePath, "blocks.bin"), 0666)
	if err != nil {
		sc.Close()
		return nil, err
	}
	sc.Blocks = bf

	tf := &TreeFileManager{}
	if err := tf.Init(bf, &NullInodeMap{}); err != nil {
		sc.Close()
		return nil, err
	}

	err = sc.dataBlockCache.Open(sc.Blocks)
	if err != nil {
		sc.Close()
		return nil, err
	}

	return sc, nil
}

func (sc *StorageContext) Close() error {
	err := sc.Blocks.Close()
	if err != nil {
		return err
	}
	return nil
}

func (sc *StorageContext) Statfs() (*unix.Statfs_t, error) {
	var buf unix.Statfs_t
	return &buf, unix.Statfs(sc.BasePath, &buf)
}
