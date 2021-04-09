package fusefs

import (
	"io/ioutil"
	"time"

	"bazil.org/fuse"
	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/storage"
	"github.com/msg555/ctrfs/unix"
)

func nsTimestampToTime(nsTimestamp uint64) time.Time {
	return time.Unix(int64(nsTimestamp/1000000000), int64(nsTimestamp%1000000000))
}

func (conn *Connection) nodeToAttr(inodeId storage.InodeId, inode *storage.InodeData) fuse.Attr {
	size := inode.Size
	if unix.S_ISDIR(inode.Mode) {
		size = 1024
	}
	return fuse.Attr{
		Valid:     DURATION_DEFAULT,
		Inode:     uint64(inodeId),
		Size:      size,
		Blocks:    (size + 511) >> 9,
		Atime:     nsTimestampToTime(inode.Atim),
		Mtime:     nsTimestampToTime(inode.Mtim),
		Ctime:     nsTimestampToTime(inode.Ctim),
		Mode:      unix.UnixToFileStatMode(inode.Mode),
		Nlink:     1,
		Uid:       inode.Uid,
		Gid:       inode.Gid,
		Rdev:      uint32(inode.Dev),
		BlockSize: 1024,
	}
}

func (conn *Connection) handleAccessRequest(req *fuse.AccessRequest) error {
	inode, err := conn.GetInode(req.Node)
	if err != nil {
		return err
	}

	if !unix.TestAccess(req.Uid == inode.Uid, req.Gid == inode.Gid, inode.Mode, req.Mask) {
		return FuseError{
			source: errors.New("permission denied"),
			errno:  unix.EACCES,
		}
	}

	req.Respond()
	return nil
}

func (conn *Connection) handleLookupRequest(req *fuse.LookupRequest) error {
	inode, err := conn.GetInode(req.Node)
	if err != nil {
		return err
	}

	childInode, childInodeId, err := conn.Storage.LookupChild(inode, req.Name)
	childInodeId = conn.remapInode(childInodeId)
	if err != nil {
		return err
	}
	if childInode == nil {
		return FuseError{
			source: errors.New("file not found"),
			errno:  unix.ENOENT,
		}
	}

	req.Respond(&fuse.LookupResponse{
		Node:       fuse.NodeID(childInodeId),
		Generation: 1,
		EntryValid: DURATION_DEFAULT,
		Attr:       conn.nodeToAttr(childInodeId, childInode),
	})

	return nil
}

func (conn *Connection) handleGetattrRequest(req *fuse.GetattrRequest) error {
	inode, err := conn.GetInode(req.Node)
	if err != nil {
		return err
	}

	req.Respond(&fuse.GetattrResponse{
		Attr: conn.nodeToAttr(storage.InodeId(req.Node), inode),
	})
	return nil
}

func (conn *Connection) handleOpenRequest(req *fuse.OpenRequest) error {
	inode, err := conn.GetInode(req.Node)
	if err != nil {
		return err
	}

	if req.Dir && !unix.S_ISDIR(inode.Mode) {
		return FuseError{
			source: errors.New("not a directory"),
			errno:  unix.ENOTDIR,
		}
	}
	if req.Flags.IsWriteOnly() || req.Flags.IsReadWrite() {
		return FuseError{
			source: errors.New("read only file system"),
			errno:  unix.EROFS,
		}
	}
	if (req.Flags & (fuse.OpenAppend | fuse.OpenCreate | fuse.OpenTruncate)) != 0 {
		return FuseError{
			source: errors.New("read only file system"),
			errno:  unix.EROFS,
		}
	}

	// TODO: Ensure we havethe right permissions
	var handleID fuse.HandleID
	switch inode.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		handleID = conn.OpenHandle(&FileHandleDir{
			Conn:      conn,
			InodeData: inode,
		})
	case unix.S_IFREG:
		file, err := conn.Storage.Cas.Open(inode.Address[:])
		if err != nil {
			return err
		}

		handleID = conn.OpenHandle(&FileHandleReg{
			InodeData: inode,
			File:      file,
		})
	default:
		return errors.New("not implemented")
	}

	req.Respond(&fuse.OpenResponse{
		Handle: handleID,
		Flags:  fuse.OpenKeepCache,
	})
	return nil
}

func (conn *Connection) handleReadlinkRequest(req *fuse.ReadlinkRequest) error {
	inode, err := conn.GetInode(req.Node)
	if err != nil {
		return err
	}

	fin, err := conn.Storage.Cas.Open(inode.Address[:])
	if err != nil {
		return err
	}

	target, err := ioutil.ReadAll(fin)
	if err != nil {
		fin.Close()
		return err
	}
	err = fin.Close()
	if err != nil {
		return err
	}

	req.Respond(string(target))
	return nil
}

func (conn *Connection) handleListxattrRequest(req *fuse.ListxattrRequest) error {
	return FuseError{
		source: errors.New("xattr not supported"),
		errno:  unix.ENOTSUP,
	}
}

func (conn *Connection) handleGetxattrRequest(req *fuse.GetxattrRequest) error {
	return FuseError{
		source: errors.New("xattr not supported"),
		errno:  unix.ENOTSUP,
	}
}
