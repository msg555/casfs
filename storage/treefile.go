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

func (tm *TreeFileManager) OpenFile(inodeId InodeId) (FileObject, error) {
	// Check if file already exists in active handles
	tm.fileMapLock.Lock()
	fo, ok := tm.fileMap[inodeId]
	if ok {
		fo.addRef()
	}
	tm.fileMapLock.Unlock()

	if fo != nil {
		return fo, nil
	}

	// Otherwise we are (most likely) going to have to create this new object.
	// We will need to map the inode and load the inode stat information first.
	mappedInodeId, err := tm.inodeMap.GetMappedNode(inodeId)
	if err != nil {
		return nil, err
	}

	var inodeData InodeData
	err = tm.blocks.AccessBlock(nil, mappedInodeId, func(data []byte) (bool, error) {
		inodeData = *InodeFromBytes(data)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	dtType := (inodeData.Mode & unix.S_IFMT) >> 12

	tm.fileMapLock.Lock()
	defer tm.fileMapLock.Unlock()

	fo, ok = tm.fileMap[inodeId]

	// Check if someone else created the object underneath us.
	if ok {
		fo.addRef()
		return fo, nil
	}

	tf := TreeFileObject{
		inodeId:    mappedInodeId,
		srcInodeId: inodeId,
		refCount:   1,
		manager:    tm,
		inodeData:  inodeData,
	}
	if dtType == unix.DT_REG || dtType == unix.DT_LNK {
		fo = &TreeFileReg{TreeFileObject: tf}
	} else if dtType == unix.DT_DIR {
		fo = &TreeFileDir{TreeFileObject: tf}
	} else {
		fo = &TreeFileOther{TreeFileObject: tf}
	}
	tm.fileMap[inodeId] = fo

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

	if err := tf.ensureWritable(false); err != nil {
		return err
	}

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

func (tf *TreeFileObject) ensureWritable(needLock bool) error {
	tm := tf.manager
	if needLock {
		tf.lock.RLock()
		defer tf.lock.RUnlock()
	}

	// Check if file already has been duplicated into a writable version.
	if tf.inodeId != tf.srcInodeId {
		return nil
	}

	// Duplicate the inode block if needed. If the inode is in a writable space
	// already this is just a quick comparison.
	duppedInodeId, err := blockfile.Duplicate(tf, tm.blocks, tf.inodeId, true)
	if err != nil {
		return err
	}
	if tf.inodeId == duppedInodeId {
		return nil // inode already writable
	}
	tf.inodeId = duppedInodeId

	// Add a mapping of the source node to this newlly allocated node.
	if err := tm.inodeMap.AddMapping(tf.srcInodeId, tf.inodeId); err != nil {
		return err
	}

	// The root tree node also needs to be writable if present.
	if tf.inodeData.TreeNode == 0 {
		return nil
	}

	newTreeNode, err := blockfile.Duplicate(tf, tm.blocks, tf.inodeData.TreeNode, true)
	if err != nil {
		return err
	}
	if newTreeNode == tf.inodeData.TreeNode {
		return nil // tree node already writable
	}

	// Update inode data
	tf.inodeData.TreeNode = newTreeNode
	return tm.blocks.AccessBlock(tf, tf.inodeId, func(data []byte) (bool, error) {
		copy(data, tf.inodeData.ToBytes())
		return true, nil
	})
}

func (tf *TreeFileOther) cacheContentAddress(sc *StorageContext) ([]byte, error) {
	// TODO
	return nil, nil
}
