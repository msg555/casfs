package storage

import (
	"github.com/msg555/ctrfs/btree"
)

type DirView struct {
	Mount *MountView
	InodeData
}

func (d *DirView) ScanChildren(startName string, scanFunc func(InodeId, string, *InodeData) bool) (bool, error) {
	return d.Mount.DirentTree.Scan(d.InodeData.TreeNode, []byte(startName), func(index btree.IndexType, key []byte, val []byte) bool {
		inodeId := InodeId(index)
		if newInodeId, found := d.Mount.inodeMap.Map[inodeId]; found {
			inodeId = newInodeId
		}
		return scanFunc(inodeId, string(key), InodeFromBytes(val))
	})
}
