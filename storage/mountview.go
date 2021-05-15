package storage

import (
	"os"
	"path"

	"github.com/go-errors/errors"
	"github.com/google/uuid"

	"github.com/msg555/ctrfs/blockfile"
)

const (
	blockIndexMeta = 1
	blockIndexRemapTree = 2
)

type MountView struct {
	ID          uuid.UUID
	RootInode   InodeData
	ReadOnly    bool
	Storage     *StorageContext
	FileManager TreeFileManager
	Blocks     blockfile.BlockAllocator
	InodeMap
}

// Creates a new empty mount. This mount does not have a root inode and it
// should be created and set by the caller.
func (sc *StorageContext) CreateEmptyMount() (*MountView, error) {
	id := uuid.New()

	// Create new block file for the mount.
	blockFilePath := path.Join(sc.BasePath, "mounts", id.String())
	bf := &blockfile.BlockFile{
		Cache: sc.Cache,
		PreAllocatedBlocks: 2,
	}
	if err := bf.Open(blockFilePath, 0666); err != nil {
		return nil, err
	}

	imap := &InodeTreeMap{}
	if err := imap.Init(bf, blockIndexRemapTree); err != nil {
		bf.Close()
		return nil, err
	}

	mnt := &MountView{
		ID: id,
		ReadOnly: false,
		Storage: sc,
		Blocks: bf,
		InodeMap: imap,
	}

	if err := mnt.FileManager.Init(bf, imap); err != nil {
		bf.Close()
		return nil, err
	}

		RootInode: 3,

	return mnt, nil
}

func (sc *StorageContext) CreateMount(rootAddress []byte, readOnly bool) (*MountView, error) {
	rootInodeId, err := sc.lookupAddressInode(rootAddress)
	if err != nil {
		return nil, err
	} else if rootInodeId == 0 {
		return nil, errors.New("could not find root content address")
	}

	id := uuid.New()
	mnt := &MountView{
		ID:       id,
		ReadOnly: readOnly,
		Storage:  sc,
	}

/*
	if rootInode.Mode == MODE_HARDLINK_LAYER {
		// TODO
	}
	mnt.RootInode = *rootInode

	if !readOnly {
		mnt.WritePath = path.Join(sc.BasePath, "mounts", id.String())

		err := os.Mkdir(mnt.WritePath, 0777)
		if err != nil {
			return nil, err
		}

		ioutil.WriteFile(path.Join(mnt.WritePath, "root"), rootAddress, 0666)

		bf := &blockfile.BlockFile{
			Cache: sc.Cache,
		}
		err = bf.Open(path.Join(mnt.WritePath, "blocks"), 0666)
		if err != nil {
			return nil, err
		}

		blockOverlay := &blockfile.BlockOverlayAllocator{}
		err = blockOverlay.Init(sc.Blocks, bf)
		if err != nil {
			return nil, err
		}

		mnt.Blocks = blockOverlay
	} else {
		mnt.Blocks = sc.Blocks
	}

	// TODO: Use the actual inode map
	err = mnt.FileManager.Init(mnt.Blocks, &NullInodeMap{})
	if err != nil {
		return nil, err
	}
*/

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

func (mnt *MountView) SetRoot(inodeId InodeId) error {
}

func (mnt *MountView) Readlink(inodeData *InodeData) (string, error) {
	// TODO: Cache this in block-cache somehow?
	/*
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
	*/
	return "", nil
}

func (mnt *MountView) Destroy(commit bool) error {
	return nil
}
