package fusefs

import (
	"errors"
	"io/ioutil"
	"time"

	"encoding/hex"

	"bazil.org/fuse"
	"github.com/msg555/casfs/storage"
	"github.com/msg555/casfs/unix"
)

func nsTimestampToTime(nsTimestamp uint64) time.Time {
	return time.Unix(int64(nsTimestamp/1000000000), int64(nsTimestamp%1000000000))
}

func (conn *FuseCasfsConnection) nodeToAttr(nodeIndex storage.InodeIndex, inode *storage.InodeData) fuse.Attr {
	return fuse.Attr{
		Valid:     DURATION_DEFAULT,
		Inode:     uint64(conn.nodeIndexToNodeID(nodeIndex)),
		Size:      inode.Size,
		Blocks:    (inode.Size + 511) >> 9,
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

func (conn *FuseCasfsConnection) handleAccessRequest(req *fuse.AccessRequest) error {
	inode, err := conn.Server.Storage.ReadInode(conn.nodeIDToNodeIndex(req.Node))
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

func (conn *FuseCasfsConnection) handleLookupRequest(req *fuse.LookupRequest) error {
	inode, err := conn.Server.Storage.ReadInode(conn.nodeIDToNodeIndex(req.Node))
	if err != nil {
		return err
	}

	data, err := conn.Server.Storage.LookupChild(inode, req.Name)
	if err != nil {
		return err
	}
	if data == nil {
		return FuseError{
			source: errors.New("permission denied"),
			errno:  unix.EACCES,
		}
	}

	childInode, err := conn.Server.Storage.ReadInode(data.Inode)
	if err != nil {
		return err
	}

	req.Respond(&fuse.LookupResponse{
		Node:       conn.nodeIndexToNodeID(data.Inode),
		Generation: 1,
		EntryValid: DURATION_DEFAULT,
		Attr:       conn.nodeToAttr(data.Inode, childInode),
	})

	return nil
}

func (conn *FuseCasfsConnection) handleGetattrRequest(req *fuse.GetattrRequest) error {
	inodeIndex := conn.nodeIDToNodeIndex(req.Node)
	inode, err := conn.Server.Storage.ReadInode(inodeIndex)
	if err != nil {
		return err
	}

	req.Respond(&fuse.GetattrResponse{
		Attr: conn.nodeToAttr(inodeIndex, inode),
	})
	return nil
}

func (conn *FuseCasfsConnection) handleOpenRequest(req *fuse.OpenRequest) error {
	inode, err := conn.Server.Storage.ReadInode(conn.nodeIDToNodeIndex(req.Node))
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
		file, err := conn.Server.Storage.Cas.Open(inode.Address[:])
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

func (conn *FuseCasfsConnection) handleReadlinkRequest(req *fuse.ReadlinkRequest) error {
	inode, err := conn.Server.Storage.ReadInode(conn.nodeIDToNodeIndex(req.Node))
	if err != nil {
		println("FAIL1")
		return err
	}

	println(hex.EncodeToString(inode.Address[:]))
	fin, err := conn.Server.Storage.Cas.Open(inode.Address[:])
	if err != nil {
		println("FAIL2", err)
		return err
	}

	target, err := ioutil.ReadAll(fin)
	println("GOT TARGET", target)
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

func (conn *FuseCasfsConnection) handleListxattrRequest(req *fuse.ListxattrRequest) error {
	return FuseError{
		source: errors.New("xattr not supported"),
		errno:  unix.ENOTSUP,
	}
}

func (conn *FuseCasfsConnection) handleGetxattrRequest(req *fuse.GetxattrRequest) error {
	return FuseError{
		source: errors.New("xattr not supported"),
		errno:  unix.ENOTSUP,
	}
}
