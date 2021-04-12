package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/go-errors/errors"
	"github.com/google/uuid"

	"github.com/msg555/ctrfs/blockfile"
	"github.com/msg555/ctrfs/btree"
)

type FileView interface {
	Close() error

	GetInode() InodeData
	UpdateInode(func(*InodeData) error) error

	Sync() error

	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
}

type fileViewRef struct {
	Cnt   int
	Rfile FileView
	Wfile FileView
}

type dirViewRef struct {
	Cnt int
	DirView
}

type MountView struct {
	ID        uuid.UUID
	RootInode InodeData
	ReadOnly  bool
	WritePath string
	Storage   *StorageContext

	blockfile.BlockFile
	DirentTree *btree.BTree

	inodeMap     InodeMap
	fileViewMap  map[InodeId]*fileViewRef
	fileViewLock sync.Mutex

	dirViewMap  map[InodeId]*dirViewRef
	dirViewLock sync.Mutex
}

func (sc *StorageContext) CreateMount(rootAddress []byte, readOnly bool) (*MountView, error) {
	rootInode, err := sc.LookupAddressInode(rootAddress)
	if err != nil {
		return nil, err
	} else if rootInode == nil {
		return nil, errors.New("could not find root content address")
	}

	id := uuid.New()
	mnt := &MountView{
		ID:       id,
		ReadOnly: readOnly,
		Storage:  sc,
		BlockFile: blockfile.BlockFile{
			Cache: sc.Cache,
		},
		fileViewMap: make(map[InodeId]*fileViewRef),
		dirViewMap:  make(map[InodeId]*dirViewRef),
	}

	if rootInode.Mode == MODE_HARDLINK_LAYER {
		newRootInode, err := sc.LookupAddressInode(rootInode.PathHash[:])
		if err != nil {
			return nil, err
		} else if newRootInode == nil {
			return nil, errors.New("reference data layer missing")
		}

		// Read hlmap from Address
		hlf, err := sc.Cas.Open(rootInode.XattrAddress[:])
		if hlf == nil {
			return nil, errors.New("could not read inode map")
		}
		err = mnt.inodeMap.Read(hlf)
		if err != nil {
			hlf.Close()
			return nil, err
		}
		err = hlf.Close()
		if err != nil {
			return nil, err
		}

		rootInode = newRootInode
	}
	mnt.RootInode = *rootInode

	if !readOnly {
		mnt.WritePath = path.Join(sc.BasePath, "mounts", id.String())

		err := os.Mkdir(mnt.WritePath, 0777)
		if err != nil {
			return nil, err
		}

		ioutil.WriteFile(path.Join(mnt.WritePath, "root"), rootAddress, 0666)

		err = mnt.BlockFile.Open(path.Join(mnt.WritePath, "blocks"), 0666)
		if err != nil {
			return nil, err
		}

		mnt.DirentTree = &btree.BTree{
			MaxKeySize:   sc.DirentTree.MaxKeySize,
			EntrySize:    sc.DirentTree.EntrySize,
			FanOut:       sc.DirentTree.FanOut,
			MaxForkDepth: sc.DirentTree.MaxForkDepth,
		}
		err = mnt.DirentTree.ForkFrom(&sc.DirentTree, &mnt.BlockFile)
		if err != nil {
			return nil, err
		}
	} else {
		mnt.DirentTree = &sc.DirentTree
	}

	return mnt, nil
}

func (sc *StorageContext) OpenMount(id uuid.UUID) (*MountView, error) {
	mnt := &MountView{
		ID:        id,
		WritePath: path.Join(sc.BasePath, "mounts", id.String()),
		Storage:   sc,
	}

	st, err := os.Stat(mnt.WritePath)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, errors.New("mount must be directory")
	}

	return mnt, nil
}

func (mnt *MountView) LookupChild(inode *InodeData, name string) (*InodeData, InodeId, error) {
	childInode, childInodeId, err := mnt.Storage.LookupChild(inode, name)
	if err != nil {
		return nil, 0, err
	}
	if childInode == nil {
		return nil, 0, nil
	}
	if newInodeId, found := mnt.inodeMap.Map[childInodeId]; found {
		childInodeId = newInodeId
	}
	return childInode, childInodeId, err
}

func (mnt *MountView) GetInode(inodeId InodeId) (*InodeData, error) {
	return mnt.Storage.ReadInode(inodeId)
}

func (mnt *MountView) Readlink(inodeData *InodeData) (string, error) {
	// TODO: Cache this in block-cache somehow?
	fin, err := mnt.Storage.Cas.Open(inodeData.Address[:])
	if err != nil {
		return "", err
	}
	defer fin.Close()

	target, err := ioutil.ReadAll(fin)
	if err != nil {
		return "", err
	}

	return string(target), nil
}

func (mnt *MountView) GetFileView(inodeId InodeId, inodeData *InodeData) (FileView, error) {
	mnt.fileViewLock.Lock()
	defer mnt.fileViewLock.Unlock()

	ref, ok := mnt.fileViewMap[inodeId]
	if !ok {
		ref = &fileViewRef{
			Cnt: 1,
		}
		f, err := mnt.Storage.Cas.Open(inodeData.Address[:])
		if err != nil {
			return nil, err
		}

		ref.Rfile = OpenROFileOverlayFromFile(f, inodeData, mnt.Storage.Cache, inodeData.Address)
		if !mnt.ReadOnly {
			wfile, err := OpenFileOverlay(ref.Rfile, path.Join(mnt.WritePath, fmt.Sprintf("%d.dif", inodeId)), 0666, mnt.Storage.Cache)
			if err != nil {
				return nil, err
			}
			ref.Wfile = wfile
		}
		mnt.fileViewMap[inodeId] = ref
	} else {
		ref.Cnt++
	}

	if mnt.ReadOnly {
		return ref.Rfile, nil
	}
	return ref.Wfile, nil
}

func (mnt *MountView) ReleaseFileView(inodeId InodeId) error {
	mnt.fileViewLock.Lock()
	defer mnt.fileViewLock.Unlock()

	ref, ok := mnt.fileViewMap[inodeId]
	if !ok {
		panic("release of file view for inode that has no active view")
	}

	ref.Cnt--
	if ref.Cnt > 0 {
		return nil
	}
	delete(mnt.fileViewMap, inodeId)

	return ref.Rfile.Close()
}

func (mnt *MountView) GetDirView(inodeId InodeId, inodeData *InodeData) (*DirView, error) {
	mnt.dirViewLock.Lock()
	defer mnt.dirViewLock.Unlock()

	ref, ok := mnt.dirViewMap[inodeId]
	if !ok {
		ref = &dirViewRef{
			Cnt: 1,
			DirView: DirView{
				Mount:     mnt,
				InodeData: *inodeData,
			},
		}

		mnt.dirViewMap[inodeId] = ref
	} else {
		ref.Cnt++
	}

	return &ref.DirView, nil
}

func (mnt *MountView) ReleaseDirView(inodeId InodeId) error {
	mnt.dirViewLock.Lock()
	defer mnt.dirViewLock.Unlock()

	ref, ok := mnt.dirViewMap[inodeId]
	if !ok {
		panic("release of dir view for inode that has no active view")
	}

	ref.Cnt--
	if ref.Cnt > 0 {
		return nil
	}
	delete(mnt.dirViewMap, inodeId)

	return nil
}
