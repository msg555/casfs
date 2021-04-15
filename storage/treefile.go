package storage

import (
	"sync"

	"github.com/msg555/ctrfs/blockfile"
	"github.com/msg555/ctrfs/btree"
	"github.com/msg555/ctrfs/unix"
)

/*
type FileView interface {
	Close() error

	GetInode() InodeData
	UpdateInode(func(*InodeData) error) error

	Sync() error

	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
}
*/

type TreeFileManager struct {
	blocks    blockfile.BlockAllocator
	blockTree btree.BTree

	inodeMap InodeMap

	fileMapLock sync.Mutex
	fileMap     map[InodeId]*TreeFile
}

type TreeFile struct {
	inodeId    InodeId
	srcInodeId InodeId

	refCount  int
	lock      sync.RWMutex
	inodeData InodeData
	manager   *TreeFileManager

	initialized bool
}

func (tm *TreeFileManager) Init(blocks blockfile.BlockAllocator, inodeMap InodeMap) error {
	tm.blocks = blocks
	tm.blockTree = btree.BTree{
		MaxKeySize: 8,
		EntrySize:  8,
	}
	tm.inodeMap = inodeMap
	tm.fileMap = make(map[InodeId]*TreeFile)

	return tm.blockTree.Open(blocks)
}

func (tm *TreeFileManager) NewFile(inodeData *InodeData) (*TreeFile, error) {
	tf := &TreeFile{
		refCount:    1,
		inodeData:   *inodeData,
		manager:     tm,
		initialized: true,
	}
	inodeId, err := tm.blocks.Allocate(tf)
	if err != nil {
		return nil, err
	}

	tf.inodeId = inodeId
	tf.srcInodeId = inodeId

	tm.fileMapLock.Lock()
	tm.fileMap[inodeId] = tf
	tm.fileMapLock.Unlock()

	return tf, nil
}

func (tm *TreeFileManager) OpenFile(inodeId InodeId) (*TreeFile, error) {
	tm.fileMapLock.Lock()
	tf, ok := tm.fileMap[inodeId]
	if !ok {
		tf = &TreeFile{
			inodeId:    inodeId,
			srcInodeId: inodeId,
			refCount:   1,
			manager:    tm,
		}
		tm.fileMap[inodeId] = tf
	} else {
		tf.refCount++
	}
	tm.fileMapLock.Unlock()

	tf.lock.Lock()
	defer tf.lock.Unlock()

	if !tf.initialized {
		cache := tm.blocks.GetCache()
		buf := cache.Pool.Get().([]byte)
		defer cache.Pool.Put(buf)

		var err error
		inodeId, err = tm.inodeMap.GetMappedNode(inodeId)
		if err != nil {
			return nil, err
		}

		_, err = tm.blocks.Read(inodeId, buf)
		if err != nil {
			return nil, err
		}
		tf.inodeData = *InodeFromBytes(buf)

		// Inode block and tree node block (if exists) need to be writable.
		tf.inodeId, err = blockfile.Duplicate(tf, tm.blocks, inodeId, true)
		if err != nil {
			return nil, err
		}
		if tf.inodeId != inodeId {
			err := tm.inodeMap.AddMapping(inodeId, tf.inodeId)
			if err != nil {
				return nil, err
			}
		}

		if tf.inodeData.TreeNode != 0 {
			newTreeNode, err := blockfile.Duplicate(tf, tm.blocks, tf.inodeData.TreeNode, true)
			if err != nil {
				return nil, err
			}
			if newTreeNode != tf.inodeData.TreeNode {
				tf.inodeData.TreeNode = newTreeNode

				// Update indoe with changed tree node
				copy(buf, tf.inodeData.ToBytes())
				if err := tm.blocks.Write(tf, tf.inodeId, buf); err != nil {
					return nil, err
				}
			}
		}

		tf.initialized = true
	}

	return tf, nil
}

func (tf *TreeFile) Close() error {
	tf.manager.fileMapLock.Lock()
	defer tf.manager.fileMapLock.Unlock()

	tf.refCount--
	if tf.refCount == 0 {
		delete(tf.manager.fileMap, tf.srcInodeId)
	}
	return nil
}

func (tf *TreeFile) GetInodeId() InodeId {
	return tf.srcInodeId
}

func (tf *TreeFile) GetInode() InodeData {
	tf.lock.RLock()
	defer tf.lock.RUnlock()
	return tf.inodeData
}

func (tf *TreeFile) UpdateInode(updateFunc func(*InodeData) error) error {
	tf.lock.Lock()
	defer tf.lock.Unlock()

	origSize := tf.inodeData.Size
	err := updateFunc(&tf.inodeData)
	if err != nil {
		return err
	}

	if tf.inodeData.Size < origSize {
		if err := tf.truncate(); err != nil {
			return err
		}
	}

	err = tf.manager.blocks.WriteAt(tf, tf.inodeId, 0, tf.inodeData.ToBytes())
	if err != nil {
		return err
	}

	return nil
}

func (tf *TreeFile) truncate() error {
	if tf.inodeData.Size == 0 {
		tf.inodeData.Blocks = 0
		if tf.inodeData.TreeNode != 0 {
			err := tf.manager.blockTree.FreeTree(tf.inodeData.TreeNode, true)
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

func (tf *TreeFile) truncateInline(lowBlock int64) error {
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

func (tf *TreeFile) truncateTree(lowBlock int64) error {
	var key [8]byte
	bo.PutUint64(key[:], uint64(lowBlock))
	for {
		k, v, _, err := tf.manager.blockTree.LowerBound(tf.inodeData.TreeNode, key[:])
		if err != nil {
			return err
		}
		if k == nil {
			return nil
		}
		err = tf.manager.blockTree.Delete(tf, tf.inodeData.TreeNode, k)
		if err != nil {
			return err
		}
		tf.manager.blocks.Free(blockfile.BlockIndex(bo.Uint64(v)))
	}
}

func (tf *TreeFile) Sync(inodeId InodeId) error {
	return tf.manager.blocks.SyncTag(tf)
}

func (tf *TreeFile) convertToTreeFile() error {
	treeRoot, err := tf.manager.blockTree.CreateEmpty(tf)
	if err != nil {
		return err
	}
	return tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		for i := 0; i < int(tf.inodeData.Blocks); i++ {
			entry := data[INODE_SIZE+i*16:]
			err := tf.manager.blockTree.Insert(tf, treeRoot, entry[:8], entry[8:16], false)
			if err != nil {
				return false, err
			}
		}
		tf.inodeData.TreeNode = treeRoot
		copy(data, tf.inodeData.ToBytes())
		return true, nil
	})
}

func (tf *TreeFile) lookupBlockInline(block int64, forWriting bool) (blockfile.BlockIndex, error) {
	var result blockfile.BlockIndex
	err := tf.manager.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		for i := 0; i < int(tf.inodeData.Blocks); i++ {
			iblk := int64(bo.Uint64(data[INODE_SIZE+i*16:]))
			if iblk == block {
				result = blockfile.BlockIndex(bo.Uint64(data[INODE_SIZE+i*16+8:]))

				// Copy on write if coming from a read only block.
				if forWriting {
					dupBlockIndex, err := blockfile.Duplicate(tf, tf.manager.blocks, result, true)
					if err != nil {
						return false, err
					}
					if result != dupBlockIndex {
						result = dupBlockIndex
						bo.PutUint64(data[INODE_SIZE+i*16+8:], uint64(dupBlockIndex))
						return true, nil
					}
				}

				return false, nil
			}
		}
		if !forWriting {
			return false, nil
		}
		if INODE_SIZE+int(tf.inodeData.Blocks+1)*16 > len(data) {
			return false, nil
		}
		blkIdx, err := tf.manager.blocks.Allocate(tf)
		if err != nil {
			return false, err
		}
		bo.PutUint64(data[INODE_SIZE+tf.inodeData.Blocks*16:], uint64(block))
		bo.PutUint64(data[INODE_SIZE+tf.inodeData.Blocks*16+8:], uint64(blkIdx))
		tf.inodeData.Blocks++
		copy(data, tf.inodeData.ToBytes())
		return true, nil
	})
	return result, err
}

func (tf *TreeFile) lookupBlockTree(block int64, forWriting bool) (blockfile.BlockIndex, error) {
	var key [8]byte
	bo.PutUint64(key[:], uint64(block))

	blockIndexBytes, _, err := tf.manager.blockTree.Find(tf.inodeData.TreeNode, key[:])
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

				err = tf.manager.blockTree.Insert(tf, tf.inodeData.TreeNode, key[:], val[:], true)
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
	err = tf.manager.blockTree.Insert(tf, tf.inodeData.TreeNode, key[:], val[:], false)
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

func (tf *TreeFile) lookupBlock(block int64, forWriting bool) (blockfile.BlockIndex, error) {
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

func (tf *TreeFile) readBlock(block int64, off int, data []byte) error {
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

func (tf *TreeFile) writeBlock(block int64, off int, data []byte) error {
	index, err := tf.lookupBlock(block, true)
	if err != nil {
		return err
	}
	return tf.manager.blocks.WriteAt(tf, index, off, data)
}

func (tf *TreeFile) ReadAt(p []byte, off int64) (int, error) {
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

func (tf *TreeFile) WriteAt(p []byte, off int64) (int, error) {
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
