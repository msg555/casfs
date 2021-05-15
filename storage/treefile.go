package storage

import (
	"io"
	"sync"

	"github.com/msg555/ctrfs/blockfile"
	"github.com/msg555/ctrfs/btree"
	"github.com/msg555/ctrfs/unix"
)

type FileObject interface {
	io.Closer
	blockObject

	GetInodeId() InodeId
	GetInode() InodeData
	UpdateInode(updateFunc func(inodeData *InodeData) error) error

	Sync() error

	addRef()
}

type FileObjectReg interface {
	FileObject
	io.ReaderAt
	io.WriterAt
	io.Reader
	io.Writer
	io.Seeker
}

type FileObjectDir interface {
	FileObject

	Lookup(name string) (dtType int, inodeId InodeId, err error)
	Link(name string, dtType int, inodeId InodeId, overwrite bool) error
	Unlink(name string) (bool, error)
	Scan(startName string, entryCallback func(name string, dtType int, inodeId InodeId) (contnue bool)) (complete bool, err error)
}

type FileObjectLnk interface {
	FileObject

	ReadLink() (string, error)
}

type FileObjectOther interface{ FileObject }

type TreeFileManager struct {
	blocks        blockfile.BlockAllocator
	fileBlockTree btree.BTree
	direntTree    btree.BTree

	inodeMap InodeMap

	fileMapLock sync.Mutex
	fileMap     map[InodeId]FileObject
}

type TreeFileObject struct {
	inodeId    InodeId
	srcInodeId InodeId

	refCount  int
	lock      sync.RWMutex
	inodeData InodeData
	manager   *TreeFileManager

	initialized bool
}

type TreeFileReg struct{
	TreeFileObject

	offset int64
	offsetLock      sync.RWMutex
}

type TreeFileDir struct{ TreeFileObject }
type TreeFileOther struct{ TreeFileObject }

func (tm *TreeFileManager) Init(blocks blockfile.BlockAllocator, inodeMap InodeMap) error {
	tm.blocks = blocks
	tm.fileBlockTree = btree.BTree{
		MaxKeySize: 8,
		EntrySize:  8,
	}
	tm.direntTree = btree.BTree{
		MaxKeySize: 255,
		EntrySize:  9,
	}
	tm.inodeMap = inodeMap
	tm.fileMap = make(map[InodeId]FileObject)

	err := tm.fileBlockTree.Open(blocks)
	if err != nil {
		return err
	}
	return tm.direntTree.Open(blocks)
}

func (tm *TreeFileManager) NewFile(inodeData *InodeData) (FileObject, error) {
	tf := TreeFileObject{
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

	var fo FileObject
	if unix.S_ISREG(inodeData.Mode) || unix.S_ISLNK(inodeData.Mode) {
		fo = &TreeFileReg{TreeFileObject: tf}
	} else if unix.S_ISDIR(inodeData.Mode) {
		fo = &TreeFileDir{TreeFileObject: tf}
	} else {
		fo = &TreeFileOther{TreeFileObject: tf}
	}
	if err := tm.blocks.WriteAt(tf, inodeId, 0, tf.inodeData.ToBytes()); err != nil {
		tm.blocks.Free(inodeId)
		return nil, err
	}

	tm.fileMapLock.Lock()
	tm.fileMap[inodeId] = fo
	tm.fileMapLock.Unlock()

	return fo, nil
}

func (tm *TreeFileManager) OpenFile(dtType int, inodeId InodeId) (FileObject, error) {
	var tf *TreeFileObject

	tm.fileMapLock.Lock()
	fo, ok := tm.fileMap[inodeId]
	if !ok {
		tf = &TreeFileObject{
			inodeId:    inodeId,
			srcInodeId: inodeId,
			refCount:   1,
			manager:    tm,
		}
		if dtType == unix.DT_REG {
			tfi := &TreeFileReg{TreeFileObject: *tf}
			tf = &tfi.TreeFileObject
			fo = tfi
		} else if dtType == unix.DT_DIR {
			tfi := &TreeFileDir{TreeFileObject: *tf}
			tf = &tfi.TreeFileObject
			fo = tfi
		} else if dtType == unix.DT_LNK {
			tfi := &TreeFileLnk{TreeFileObject: *tf}
			tf = &tfi.TreeFileObject
			fo = tfi
		} else {
			tfi := &TreeFileOther{TreeFileObject: *tf}
			tf = &tfi.TreeFileObject
			fo = tfi
		}
		tm.fileMap[inodeId] = fo
	} else {
		fo.addRef()
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

		if int((tf.inodeData.Mode&unix.S_IFMT)>>12) != dtType {
			panic("opening file with wrong type")
		}

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

	return fo, nil
}

func (tf *TreeFileObject) Close() error {
	tf.manager.fileMapLock.Lock()
	defer tf.manager.fileMapLock.Unlock()

	tf.refCount--
	if tf.refCount == 0 {
		delete(tf.manager.fileMap, tf.srcInodeId)
	}
	return nil
}

func (tf *TreeFileObject) GetInodeId() InodeId {
	return tf.srcInodeId
}

func (tf *TreeFileObject) GetInode() InodeData {
	tf.lock.RLock()
	defer tf.lock.RUnlock()
	return tf.inodeData
}

func (tf *TreeFileObject) UpdateInode(updateFunc func(*InodeData) error) error {
	tf.lock.Lock()
	defer tf.lock.Unlock()

	err := updateFunc(&tf.inodeData)
	if err != nil {
		return err
	}

	err = tf.manager.blocks.WriteAt(tf, tf.inodeId, 0, tf.inodeData.ToBytes())
	if err != nil {
		return err
	}

	return nil
}

func (tf *TreeFileObject) Sync() error {
	return tf.manager.blocks.SyncTag(tf)
}

func (tf *TreeFileObject) addRef() {
	tf.refCount++
}

func (tf *TreeFileOther) cacheContentAddress(sc *StorageContext) ([]byte, error) {
	// TODO
	return nil, nil
}
