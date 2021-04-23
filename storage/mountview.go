package storage

import (
	"os"
	"path"

	"github.com/go-errors/errors"
	"github.com/google/uuid"

	"github.com/msg555/ctrfs/blockfile"
)

type MountView struct {
	ID          uuid.UUID
	RootInode   InodeData
	ReadOnly    bool
	WritePath   string
	Storage     *StorageContext
	FileManager TreeFileManager

	Blocks     blockfile.BlockAllocator

	inodeMap InodeMap
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
