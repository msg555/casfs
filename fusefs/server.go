package fusefs

import (
	"time"

	"bazil.org/fuse"
	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/storage"
)

const DURATION_DEFAULT time.Duration = time.Duration(1000000000 * 60 * 60)

type FuseCasfsServer struct {
	Storage *storage.StorageContext
	Fail    chan error
}

func CreateDefaultServer() (*FuseCasfsServer, error) {
	sc, err := storage.OpenDefaultStorageContext()
	if err != nil {
		return nil, err
	}

	return &FuseCasfsServer{
		Storage: sc,
		Fail:    make(chan error),
	}, nil
}

func (srv *FuseCasfsServer) Mount(mountPoint string, contentAddress []byte, readOnly bool, options ...fuse.MountOption) (*FuseCasfsConnection, error) {
	rootInode, err := srv.Storage.LookupAddressInode(contentAddress)
	if err != nil {
		return nil, err
	} else if rootInode == nil {
		return nil, errors.New("could not find root content address")
	}

	var inodeMap *storage.InodeMap
	if rootInode.Mode == storage.MODE_HARDLINK_LAYER {
		newRootInode, err := srv.Storage.LookupAddressInode(rootInode.PathHash[:])
		if err != nil {
			return nil, err
		} else if newRootInode == nil {
			return nil, errors.New("reference data layer missing")
		}

		// Read hlmap from Address
		inodeMap = &storage.InodeMap{}
		hlf, err := srv.Storage.Cas.Open(rootInode.XattrAddress[:])
		if hlf == nil {
			return nil, errors.New("could not read inode map")
		}
		err = inodeMap.Read(hlf)
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

	if readOnly {
		options = append(options, fuse.ReadOnly())
	}
	conn, err := fuse.Mount(mountPoint, options...)
	if err != nil {
		return nil, err
	}

	return &FuseCasfsConnection{
		Conn:       conn,
		Server:     srv,
		MountPoint: mountPoint,
		ReadOnly:   readOnly,
		rootInode:  *rootInode,
		inodeMap:   inodeMap,
		handleMap:  make(map[fuse.HandleID]Handle),
	}, nil
}
