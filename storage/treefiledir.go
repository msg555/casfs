package storage

import (
	"strings"

	"github.com/msg555/ctrfs/btree"
)

func (tf *TreeFileDir) convertToTreeFile() error {
	treeRoot, err := tf.manager.direntTree.CreateEmpty(tf)
	if err != nil {
		return err
	}
	return tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		pos := INODE_SIZE
		for pos < len(data) {
			nameLen := int(data[pos])
			if nameLen == 0 {
				break
			}
			if pos+nameLen+10 > len(data) {
				break
			}

			entryName := data[pos+1 : pos+1+nameLen]
			val := data[pos+1+nameLen : pos+10+nameLen]
			err := tf.manager.direntTree.Insert(tf, treeRoot, entryName, val, false)
			if err != nil {
				return false, err
			}
			pos += 10 + nameLen
		}
		tf.inodeData.TreeNode = treeRoot
		copy(data, tf.inodeData.ToBytes())
		return true, nil
	})
}

func (tf *TreeFileDir) lookupInline(name string) (int, InodeId, error) {
	var dtType int
	var inodeId InodeId
	err := tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		for pos := INODE_SIZE; pos < len(data); {
			nameLen := int(data[pos])
			if nameLen == 0 {
				break
			}
			if pos+nameLen+10 > len(data) {
				break
			}

			entryName := string(data[pos+1 : pos+1+nameLen])
			if name == entryName {
				inodeId = InodeId(bo.Uint64(data[pos+1+nameLen:]))
				dtType = int(data[pos+9+nameLen])
				return false, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return 0, 0, err
	}
	return dtType, inodeId, err
}

func (tf *TreeFileDir) lookupTree(name string) (int, InodeId, error) {
	val, _, err := tf.manager.direntTree.Find(tf.inodeData.TreeNode, []byte(name))
	if err != nil {
		return 0, 0, err
	}
	if val == nil {
		return 0, 0, nil
	}
	return int(val[8]), InodeId(bo.Uint64(val)), nil
}

func (tf *TreeFileDir) Lookup(name string) (int, InodeId, error) {
	if tf.inodeData.TreeNode == 0 {
		return tf.lookupInline(name)
	}
	return tf.lookupTree(name)
}

func (tf *TreeFileDir) linkInline(name string, dtType int, inodeId InodeId, overwrite bool) (bool, error) {
	updated := false
	err := tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		insertPos := 0
		pos := INODE_SIZE
		for pos < len(data) {
			nameLen := int(data[pos])
			if nameLen == 0 {
				break
			}
			if pos+nameLen+10 > len(data) {
				break
			}

			entryName := string(data[pos+1 : pos+1+nameLen])
			cmp := strings.Compare(name, entryName)
			if cmp < 0 {
				insertPos = pos
			} else if cmp == 0 {
				bo.PutUint64(data[pos+1+nameLen:], uint64(inodeId))
				data[pos+9+nameLen] = byte(dtType)

				updated = true
				return true, nil
			}

			pos += nameLen + 10
		}
		if pos+10+len(name) > len(data) {
			// No room for new entry
			return false, nil
		}
		if insertPos == 0 {
			// Element fits in at end of list
			insertPos = pos
		}
		// Rotate elements toward the back
		copy(data[insertPos+10+len(name):], data[insertPos:pos])

		// Write new entry
		data[insertPos] = byte(len(name))
		copy(data[insertPos+1:], name)
		bo.PutUint64(data[insertPos+1+len(name):], uint64(inodeId))
		data[insertPos+9+len(name)] = byte(dtType)
		return false, nil
	})
	return updated, err
}

func (tf *TreeFileDir) linkTree(name string, dtType int, inodeId InodeId, overwrite bool) error {
	var entry [9]byte
	bo.PutUint64(entry[:], uint64(inodeId))
	entry[8] = byte(dtType)
	err := tf.manager.direntTree.Insert(tf, tf.inodeData.TreeNode, []byte(name), entry[:], overwrite)
	if !overwrite && err == btree.ErrorKeyAlreadyExists {
		return nil
	}
	return err
}

func (tf *TreeFileDir) Link(name string, dtType int, inodeId InodeId, overwrite bool) error {
	if tf.inodeData.TreeNode == 0 {
		written, err := tf.linkInline(name, dtType, inodeId, overwrite)
		if err != nil {
			return err
		}
		if written {
			return nil
		}
		if err := tf.convertToTreeFile(); err != nil {
			return err
		}
	}
	return tf.linkTree(name, dtType, inodeId, overwrite)
}

func (tf *TreeFileDir) unlinkInline(name string) (bool, error) {
	found := false
	err := tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		pos := INODE_SIZE
		for pos < len(data) {
			nameLen := int(data[pos])
			if nameLen == 0 {
				break
			}
			if pos+nameLen+10 > len(data) {
				break
			}

			entryName := string(data[pos+1 : pos+1+nameLen])
			if name == entryName {
				// Rotate entry list forward
				found = true
				copy(data[pos:], data[pos+nameLen+10:])
				return true, nil
			}

			pos += nameLen + 10
		}
		return false, nil
	})
	return found, err
}

func (tf *TreeFileDir) unlinkTree(name string) (bool, error) {
	err := tf.manager.direntTree.Delete(tf, tf.inodeData.TreeNode, []byte(name))
	if err == btree.ErrorKeyNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (tf *TreeFileDir) Unlink(name string) (bool, error) {
	if tf.inodeData.TreeNode == 0 {
		return tf.unlinkInline(name)
	}
	return tf.unlinkTree(name)
}

func (tf *TreeFileDir) scanInline(startName string, entryCallback func(name string, dtType int, inodeId InodeId) bool) (bool, error) {
	var complete bool
	err := tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		started := false
		for pos := INODE_SIZE; pos < len(data); {
			nameLen := int(data[pos])
			if nameLen == 0 {
				break
			}
			if pos+nameLen+10 > len(data) {
				break
			}

			entryName := string(data[pos+1 : pos+1+nameLen])
			if !started {
				started = startName <= entryName
			}
			if started {
				ctn := entryCallback(entryName, int(data[pos+9+nameLen]), InodeId(bo.Uint64(data[pos+1+nameLen:])))
				if !ctn {
					break
				}
			}
		}
		complete = true
		return false, nil
	})
	if err != nil {
		return false, err
	}
	return complete, nil
}

func (tf *TreeFileDir) scanTree(startName string, entryCallback func(name string, dtType int, inodeId InodeId) bool) (bool, error) {
	return tf.manager.direntTree.Scan(tf.inodeData.TreeNode, []byte(startName), func(_ btree.IndexType, entryName []byte, entryValue []byte) bool {
		return entryCallback(string(entryName), int(entryValue[8]), InodeId(bo.Uint64(entryValue)))
	})
}

func (tf *TreeFileDir) Scan(startName string, entryCallback func(name string, dtType int, inodeId InodeId) bool) (bool, error) {
	if tf.inodeData.TreeNode == 0 {
		return tf.scanInline(startName, entryCallback)
	}
	return tf.scanTree(startName, entryCallback)
}

func (tf *TreeFileDir) cacheContentAddress(sc* StorageContext) ([]byte, error) {
	// TODO
	return nil, nil
}
