package storage

import (
	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/blockfile"
	"github.com/msg555/ctrfs/btree"
)

type InodeMap interface {
	AddMapping(srcInodeId, dstInodeId InodeId) error
	GetMappedNode(srcInodeId InodeId) (InodeId, error)
}

type NullInodeMap struct{}

func (mp *NullInodeMap) AddMapping(srcInodeId, dstInodeId InodeId) error {
	return errors.New("cannot map inodes")
}

func (mp *NullInodeMap) GetMappedNode(srcInodeId InodeId) (InodeId, error) {
	return srcInodeId, nil
}

type InodeTreeMap struct {
	blocks   blockfile.BlockAllocator
	treeRoot btree.TreeIndex
	tree     btree.BTree
}

func (mp *InodeTreeMap) Init(bf blockfile.BlockAllocator, treeRoot btree.TreeIndex) error {
	mp.blocks = bf
	mp.tree = btree.BTree{
		MaxKeySize: 8,
		EntrySize:  8,
	}
	err := mp.tree.Open(bf)
	if err != nil {
		return err
	}

	if treeRoot == 0 {
		mp.treeRoot, err = mp.tree.CreateEmpty(mp)
		return err
	}
	mp.treeRoot = treeRoot
	return nil
}

func (mp *InodeTreeMap) AddMapping(srcInodeId InodeId, dstInodeId InodeId) error {
	var key, val [8]byte
	bo.PutUint64(key[:], uint64(srcInodeId))
	bo.PutUint64(val[:], uint64(dstInodeId))
	return mp.tree.Insert(mp, mp.treeRoot, key[:], val[:], false)
}

func (mp *InodeTreeMap) GetMappedNode(srcInodeId InodeId) (InodeId, error) {
	var key [8]byte
	bo.PutUint64(key[:], uint64(srcInodeId))
	val, _, err := mp.tree.Find(mp.treeRoot, key[:])
	if err != nil {
		return 0, err
	}
	if val == nil {
		return srcInodeId, nil
	}
	return InodeId(bo.Uint64(val)), nil
}

func (mp *InodeTreeMap) Sync() error {
	return mp.blocks.SyncTag(mp)
}
