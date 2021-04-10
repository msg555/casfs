package fusefs

//EROFS

import (
	"fmt"
	"sync"

	"bazil.org/fuse"
	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/storage"
	"github.com/msg555/ctrfs/unix"
)

const FUSE_ROOT_ID fuse.NodeID = 1
const FUSE_UNKNOWN_INO fuse.NodeID = 0xffffffff

type Connection struct {
	Conn       *fuse.Conn
	MountPoint string
	ReadOnly   bool
	Mount      *storage.MountView

	handleLock   sync.RWMutex
	handleMap    map[fuse.HandleID]Handle
	lastHandleID fuse.HandleID
}

func (conn *Connection) Serve() error {
	for {
		req, err := conn.Conn.ReadRequest()
		if err != nil {
			return err
		}
		go conn.handleRequest(req)
	}
}

func (conn *Connection) GetInode(inodeId fuse.NodeID) (*storage.InodeData, error) {
	if inodeId == FUSE_ROOT_ID {
		return &conn.Mount.RootInode, nil
	}
	return conn.Mount.GetInode(storage.InodeId(inodeId))
}

func (conn *Connection) handleRequest(req fuse.Request) {
	var err error

	// fmt.Println("REQUEST:", req)
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
	case *fuse.WriteRequest:
		err = conn.handleWriteRequest(req.(*fuse.WriteRequest))
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

	// Not implemented/rely on default kernel level behavior. These failures are
	// cached by the fuse-driver and future calls will be automatically skipped.
	case *fuse.PollRequest:
		err = FuseError{
			source: errors.New("not implemented"),
			errno:  unix.ENOSYS,
		}

	case *fuse.DestroyRequest:
		fmt.Println("TODO: Got destroy request")

	default:
		fmt.Println("WARNING NOT IMPLEMENTED:", req)
		err = errors.New("not implemented")
	}

	if err != nil {
		req.RespondError(WrapIOError(err))
	}
}

func (conn *Connection) Close() error {
	err := fuse.Unmount(conn.MountPoint)
	if err != nil {
		return err
	}
	return conn.Conn.Close()
}

func (conn *Connection) handleStatfsRequest(req *fuse.StatfsRequest) error {
	stfs, err := conn.Mount.Storage.Statfs()
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
