package storage

import (
	"github.com/msg555/ctrfs/blockfile"
	"github.com/msg555/ctrfs/btree"
	"github.com/msg555/ctrfs/unix"
)

func (tf *TreeFileReg) UpdateInode(updateFunc func(*InodeData) error) error {
	return tf.TreeFileObject.UpdateInode(func(inodeData *InodeData) error {
		origSize := inodeData.Size
		if err := updateFunc(inodeData); err != nil {
			return err
		}
		if inodeData.Size < origSize {
			if err := tf.truncate(); err != nil {
				return err
			}
		}
		return nil
	})
}

func (tf *TreeFileReg) searchBlockInline(data []byte, block int64) (int, bool) {
	lo := 0
	hi := int(tf.inodeData.Blocks)
	for lo < hi {
		md := lo + (hi - lo) / 2

		mdBlock := int64(bo.Uint64(data[INODE_SIZE+md*16:]))
		if mdBlock == block {
			return md, true
		} else if mdBlock < block {
			lo = md + 1
		} else {
			hi = md
		}
	}
	return lo, false
}

func (tf *TreeFileReg) truncate() error {
	if tf.inodeData.Size == 0 {
		tf.inodeData.Blocks = 0
		if tf.inodeData.TreeNode != 0 {
			err := tf.manager.fileBlockTree.FreeTree(tf.inodeData.TreeNode, true)
			if err != nil {
				return err
			}
			tf.inodeData.TreeNode = 0
		}
		return nil
	}

	blockSize := int64(tf.manager.blocks.GetBlockSize())

	var err error
	off := int64(tf.inodeData.Size) % blockSize
	if off != 0 {
		buf := tf.manager.blocks.GetCache().Pool.Get().([]byte)
		defer tf.manager.blocks.GetCache().Pool.Put(buf)
		for i := 0; i < int(blockSize-off); i++ {
			buf[i] = 0
		}

		lastBlock := (int64(tf.inodeData.Size) - 1) / blockSize
		err = tf.writeBlock(lastBlock, int(off), buf[:blockSize-off])
		if err != nil {
			return err
		}
	}

	if tf.inodeData.TreeNode == 0 {
		err = tf.truncateInline((int64(tf.inodeData.Size) + blockSize - 1) / blockSize)
	} else {
		err = tf.truncateTree((int64(tf.inodeData.Size) + blockSize - 1) / blockSize)
	}
	return err
}

func (tf *TreeFileReg) truncateInline(lowBlock int64) error {
	return tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		writeInd := 0
		for i := 0; i < int(tf.inodeData.Blocks); i++ {
			buf := data[INODE_SIZE+i*16 : INODE_SIZE+(i+1)*16]
			blk := int64(bo.Uint64(buf))
			if blk < lowBlock {
				if writeInd != i {
					copy(data[INODE_SIZE+writeInd*16:], buf)
				}
				writeInd++
			} else {
				tf.manager.blocks.Free(int64(bo.Uint64(buf[8:])))
			}
		}
		tf.inodeData.Blocks = uint64(writeInd)
		return true, nil
	})
}

func (tf *TreeFileReg) truncateTree(lowBlock int64) error {
	var key [8]byte
	bo.PutUint64(key[:], uint64(lowBlock))
	for {
		k, v, _, err := tf.manager.fileBlockTree.LowerBound(tf.inodeData.TreeNode, key[:])
		if err != nil {
			return err
		}
		if k == nil {
			return nil
		}
		err = tf.manager.fileBlockTree.Delete(tf, tf.inodeData.TreeNode, k)
		if err != nil {
			return err
		}
		tf.manager.blocks.Free(blockfile.BlockIndex(bo.Uint64(v)))
	}
}

func (tf *TreeFileReg) convertToTreeFile() error {
	treeRoot, err := tf.manager.fileBlockTree.CreateEmpty(tf)
	if err != nil {
		return err
	}
	return tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		for i := 0; i < int(tf.inodeData.Blocks); i++ {
			entry := data[INODE_SIZE+i*16:]
			err := tf.manager.fileBlockTree.Insert(tf, treeRoot, entry[:8], entry[8:16], false)
			if err != nil {
				return false, err
			}
		}
		tf.inodeData.TreeNode = treeRoot
		copy(data, tf.inodeData.ToBytes())
		return true, nil
	})
}

func (tf *TreeFileReg) lookupBlockInline(block int64, forWriting bool) (blockfile.BlockIndex, error) {
	var result blockfile.BlockIndex
	err := tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		insertInd, match := tf.searchBlockInline(data, block)
		insertData := data[INODE_SIZE+insertInd*16:]

		if match {
			result = blockfile.BlockIndex(bo.Uint64(insertData[8:]))

			// Copy on write if coming from a read only block.
			if forWriting {
				dupBlockIndex, err := blockfile.Duplicate(tf, tf.manager.blocks, result, true)
				if err != nil {
					return false, err
				}
				if result != dupBlockIndex {
					result = dupBlockIndex
					bo.PutUint64(insertData[8:], uint64(dupBlockIndex))
					return true, nil
				}
			}
			return false, nil
		}
		if !forWriting {
			return false, nil
		}

		if INODE_SIZE+int(tf.inodeData.Blocks+1)*16 > len(data) {
			// No more space for an inline block
			return false, nil
		}

		// Create new block and insert it into the block list.
		blkIdx, err := tf.manager.blocks.Allocate(tf)
		if err != nil {
			return false, err
		}
		copy(insertData[16:16*(int(tf.inodeData.Blocks)-insertInd+1)], insertData)
		bo.PutUint64(insertData, uint64(block))
		bo.PutUint64(insertData[8:], uint64(blkIdx))

		// Update inode block count
		tf.inodeData.Blocks++
		copy(data, tf.inodeData.ToBytes())

		return true, nil
	})
	return result, err
}

func (tf *TreeFileReg) lookupBlockTree(block int64, forWriting bool) (blockfile.BlockIndex, error) {
	var key [8]byte
	bo.PutUint64(key[:], uint64(block))

	blockIndexBytes, _, err := tf.manager.fileBlockTree.Find(tf.inodeData.TreeNode, key[:])
	if err != nil {
		return 0, err
	}
	if blockIndexBytes != nil {
		blockIndex := blockfile.BlockIndex(bo.Uint64(blockIndexBytes))

		// Copy on write if coming from a read only block.
		if forWriting {
			dupBlockIndex, err := blockfile.Duplicate(tf, tf.manager.blocks, blockIndex, true)
			if err != nil {
				return 0, err
			}
			if blockIndex != dupBlockIndex {
				var val [8]byte
				bo.PutUint64(val[:], uint64(dupBlockIndex))

				err = tf.manager.fileBlockTree.Insert(tf, tf.inodeData.TreeNode, key[:], val[:], true)
				if err != nil {
					return 0, err
				}
				blockIndex = dupBlockIndex
			}
		}

		return blockIndex, nil
	}
	if !forWriting {
		return 0, nil
	}

	blockIndex, err := tf.manager.blocks.Allocate(tf)
	if err != nil {
		return 0, err
	}

	var val [8]byte
	bo.PutUint64(val[:], uint64(blockIndex))
	err = tf.manager.fileBlockTree.Insert(tf, tf.inodeData.TreeNode, key[:], val[:], false)
	if err != nil {
		return 0, err
	}

	tf.inodeData.Blocks++
	err = tf.manager.blocks.WriteAt(tf, tf.inodeId, 0, tf.inodeData.ToBytes())
	if err != nil {
		return 0, err
	}

	return blockIndex, nil
}

func (tf *TreeFileReg) lookupBlock(block int64, forWriting bool) (blockfile.BlockIndex, error) {
	if tf.inodeData.TreeNode == 0 {
		blockIndex, err := tf.lookupBlockInline(block, forWriting)
		if err != nil {
			return 0, err
		}
		if blockIndex != 0 || !forWriting {
			return blockIndex, nil
		}

		err = tf.convertToTreeFile()
		if err != nil {
			return 0, err
		}
	}

	return tf.lookupBlockTree(block, forWriting)
}

func (tf *TreeFileReg) readBlock(block int64, off int, data []byte) error {
	index, err := tf.lookupBlock(block, false)
	if err != nil {
		return err
	}
	if index == 0 {
		// Block does not exist, treat as zeroes
		for i := 0; i < len(data); i++ {
			data[i] = 0
		}
		return nil
	}
	_, err = tf.manager.blocks.ReadAt(index, off, len(data), data)
	return err
}

func (tf *TreeFileReg) writeBlock(block int64, off int, data []byte) error {
	index, err := tf.lookupBlock(block, true)
	if err != nil {
		return err
	}
	return tf.manager.blocks.WriteAt(tf, index, off, data)
}

func (tf *TreeFileReg) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, unix.EINVAL
	}

	tf.lock.RLock()
	defer tf.lock.RUnlock()

	if off >= int64(tf.inodeData.Size) {
		return 0, nil
	}
	if int64(len(p)) > int64(tf.inodeData.Size)-off {
		p = p[:int64(tf.inodeData.Size)-int64(off)]
	}

	blockSize := int64(tf.manager.blocks.GetBlockSize())
	dataBlockStart := off / int64(blockSize)

	read := 0
	for block := dataBlockStart; read < len(p); block++ {
		startInd := off - int64(block)*blockSize
		if startInd < 0 {
			startInd = 0
		}

		endInd := off + int64(len(p)) - int64(block)*blockSize
		if blockSize < endInd {
			endInd = blockSize
		}

		err := tf.readBlock(block, int(startInd), p[read:read+int(endInd-startInd)])
		if err != nil {
			return read, err
		}

		read += int(endInd - startInd)
	}

	return read, nil
}

func (tf *TreeFileReg) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, unix.EINVAL
	}
	if len(p) == 0 {
		// From my understanding of write(2) if zero bytes are written it will not
		// extend the file even if the file pointer is beyond the end of the file.
		return 0, nil
	}

	tf.lock.Lock()
	defer tf.lock.Unlock()

	blockSize := int64(tf.manager.blocks.GetBlockSize())
	dataBlockStart := off / int64(blockSize)

	written := 0
	for block := dataBlockStart; written < len(p); block++ {
		startInd := off - int64(block)*blockSize
		if startInd < 0 {
			startInd = 0
		}

		endInd := off + int64(len(p)) - int64(block)*blockSize
		if blockSize < endInd {
			endInd = blockSize
		}

		err := tf.writeBlock(block, int(startInd), p[written:written+int(endInd-startInd)])
		if err != nil {
			return written, err
		}

		written += int(endInd - startInd)
	}

	if uint64(off)+uint64(len(p)) > tf.inodeData.Size {
		tf.inodeData.Size = uint64(off) + uint64(len(p))
		err := tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
			copy(data, tf.inodeData.ToBytes())
			return true, nil
		})
		if err != nil {
			return 0, err
		}
	}

	return written, nil
}

func (tf *TreeFileReg) cacheContentAddress(sc *StorageContext) ([]byte, error) {
	blocks := tf.manager.blocks
	if blocks != sc.Blocks {
		panic("cannot cache content address for mount blocks")
	}

	hsh := sc.HashFactory()
	hsh.Write([]byte("file:"))

	if tf.inodeData.TreeNode == 0 {
		err := blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
			for i := 0; i < int(tf.inodeData.Blocks); i++ {
				buf := data[INODE_SIZE+i*16:]
				hsh.Write(buf[:8])

				blockNode := blockfile.BlockIndex(bo.Uint64(buf[8:]))
				if bca, err := sc.cacheDataBlockContentAddress(blockNode); err != nil {
					return false, err
				} else {
					hsh.Write(bca)
				}
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		_, err := tf.manager.fileBlockTree.Scan(tf.inodeData.TreeNode, nil, func(_ btree.IndexType, key []byte, val []byte) bool {
			hsh.Write(key)

			blockNode := blockfile.BlockIndex(bo.Uint64(val))
			if bca, err := sc.cacheDataBlockContentAddress(blockNode); err != nil {
				return false
			} else {
				hsh.Write(bca)
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	contentAddress := hsh.Sum(nil)
	if err := sc.insertBlockIntoCache(contentAddress, tf.inodeId); err != nil {
		return nil, err
	}
	return contentAddress, nil
}
