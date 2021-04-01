package fusefs

//EROFS

import (
	"fmt"
	"sync"

	"bazil.org/fuse"
	"github.com/go-errors/errors"

	"github.com/msg555/casfs/storage"
)

const FUSE_ROOT_ID fuse.NodeID = 1
const FUSE_UNKNOWN_INO fuse.NodeID = 0xffffffff

type FuseCasfsConnection struct {
	Conn       *fuse.Conn
	Server     *FuseCasfsServer
	MountPoint string
	ReadOnly   bool

	rootInode  storage.InodeData
	inodeMap   *storage.InodeMap

	handleLock   sync.RWMutex
	handleMap    map[fuse.HandleID]Handle
	lastHandleID fuse.HandleID
}

func (conn *FuseCasfsConnection) Serve() {
	for {
		req, err := conn.Conn.ReadRequest()
		if err != nil {
			conn.Server.Fail <- err
			return
		}
		go conn.handleRequest(req)
	}
}

func (conn *FuseCasfsConnection) remapInode(inodeId storage.InodeId) storage.InodeId {
	if conn.inodeMap == nil {
		return inodeId
	}
	newInodeId, found := conn.inodeMap.Map[inodeId]
	if found {
		return newInodeId
	}
	return inodeId
}

func (conn *FuseCasfsConnection) GetInode(inodeId fuse.NodeID) (*storage.InodeData, error) {
	if inodeId == FUSE_ROOT_ID {
		return &conn.rootInode, nil
	}
	return conn.Server.Storage.ReadInode(storage.InodeId(inodeId))
}

func (conn *FuseCasfsConnection) handleRequest(req fuse.Request) {
	var err error

	fmt.Println("REQUEST:", req)
	switch req.(type) {
	case *fuse.StatfsRequest:
		err = conn.handleStatfsRequest(req.(*fuse.StatfsRequest))

	// Node methods
	case *fuse.AccessRequest:
		err = conn.handleAccessRequest(req.(*fuse.AccessRequest))
	case *fuse.GetattrRequest:
		err = conn.handleGetattrRequest(req.(*fuse.GetattrRequest))
	case *fuse.LookupRequest:
		err = conn.handleLookupRequest(req.(*fuse.LookupRequest))
	case *fuse.OpenRequest:
		err = conn.handleOpenRequest(req.(*fuse.OpenRequest))
	case *fuse.ReadlinkRequest:
		err = conn.handleReadlinkRequest(req.(*fuse.ReadlinkRequest))
	case *fuse.ListxattrRequest:
		err = conn.handleListxattrRequest(req.(*fuse.ListxattrRequest))
	case *fuse.GetxattrRequest:
		err = conn.handleGetxattrRequest(req.(*fuse.GetxattrRequest))
		/*
		   case *fuse.SetattrRequest:
		     nd.handleSetattrRequest(req.(*fuse.SetattrRequest))
		   case *fuse.CreateRequest:
		     nd.handleCreateRequest(req.(*fuse.CreateRequest))
		   case *fuse.RemoveRequest:
		     nd.handleRemoveRequest(req.(*fuse.RemoveRequest))
		*/

		// fsync, forget

	// Handle methods
	case *fuse.ReadRequest:
		err = conn.handleReadRequest(req.(*fuse.ReadRequest))
	case *fuse.ReleaseRequest:
		err = conn.handleReleaseRequest(req.(*fuse.ReleaseRequest))
	case *fuse.FlushRequest:
		err = conn.handleFlushRequest(req.(*fuse.FlushRequest))
		/*
		   case *fuse.WriteRequest:
		     nd.handleWriteRequest(req.(*fuse.WriteRequest))
		   case *fuse.IoctlRequest:
		     nd.handleIoctlRequest(req.(*fuse.IoctlRequest))
		*/

	default:
		fmt.Println("WARNING NOT IMPLEMENTED:", req)
		err = errors.New("not implemented")
	}

	if err != nil {
		req.RespondError(WrapIOError(err))
	}
}

func (conn *FuseCasfsConnection) Close() error {
	err := fuse.Unmount(conn.MountPoint)
	if err != nil {
		return err
	}
	return conn.Conn.Close()
}

func (conn *FuseCasfsConnection) handleStatfsRequest(req *fuse.StatfsRequest) error {
	stfs, err := conn.Server.Storage.Statfs()
	if err != nil {
		return err
	}
	req.Respond(&fuse.StatfsResponse{
		Blocks:  stfs.Blocks,
		Bfree:   stfs.Bfree,
		Bavail:  stfs.Bavail,
		Files:   stfs.Files,
		Ffree:   stfs.Ffree,
		Bsize:   uint32(stfs.Bsize),
		Namelen: uint32(stfs.Namelen),
		Frsize:  uint32(stfs.Frsize),
	})
	return nil
}
