package fusefs

import (
	"bazil.org/fuse"
	"github.com/msg555/casfs/storage"
)

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
	}, nil
}
