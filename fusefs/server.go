package fusefs

import (
	"errors"
	"fmt"
	"time"

	"bazil.org/fuse"
	"github.com/msg555/casfs/storage"
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
	rootIndex, err := srv.Storage.LookupAddressInode(contentAddress)
	if err != nil {
		return nil, err
	} else if rootIndex == 0 {
		return nil, errors.New("could not find root content address")
	}
	fmt.Println("HELLO", rootIndex)

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
		rootIndex:  rootIndex,
		handleMap: make(map[fuse.HandleID]Handle),
	}, nil
}
