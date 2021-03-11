package main

import (
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
)

type NodeData struct {
	Cfs					*CasFS
	Node				fuse.NodeID
	Path				string
	Stat				syscall.Stat_t
	ParentNode	fuse.NodeID
}

func (nd *NodeData) TestAccess(req fuse.Request, mask uint32) bool {
	hdr := req.Hdr()
	mode := uint32(nd.Stat.Mode)

	modeEffective := mode & 07
	if hdr.Uid == nd.Stat.Uid {
		modeEffective |= (mode >> 6) & 07
	}
	if hdr.Gid == nd.Stat.Gid {
		modeEffective |= (mode >> 6) & 07
	}
	return (mask & modeEffective) == mask
}

func unixToFileStatMode(unixMode uint32) os.FileMode {
	mode := os.FileMode(unixMode & 0777)
	switch unixMode & syscall.S_IFMT {
		case syscall.S_IFBLK:
			mode |= os.ModeDevice
		case syscall.S_IFCHR:
			mode |= os.ModeDevice | os.ModeCharDevice
		case syscall.S_IFDIR:
			mode |= os.ModeDir
		case syscall.S_IFIFO:
			mode |= os.ModeNamedPipe
		case syscall.S_IFLNK:
			mode |= os.ModeSymlink
		case syscall.S_IFREG:
			// nothing to do
		case syscall.S_IFSOCK:
			mode |= os.ModeSocket
	}
	if (unixMode & syscall.S_ISGID) != 0 {
		mode |= os.ModeSetgid
	}
	if (unixMode & syscall.S_ISUID) != 0 {
		mode |= os.ModeSetuid
	}
	if (unixMode & syscall.S_ISVTX) != 0 {
		mode |= os.ModeSticky
	}
	return mode
}

func (nd *NodeData) GetAttr() fuse.Attr {
	return fuse.Attr{
		Valid: DURATION_DEFAULT,
		Inode: uint64(nd.Node),
		Size: uint64(nd.Stat.Size),
		Blocks: uint64(nd.Stat.Blocks),
		Atime: time.Unix(nd.Stat.Atim.Sec, nd.Stat.Atim.Nsec),
		Mtime: time.Unix(nd.Stat.Mtim.Sec, nd.Stat.Mtim.Nsec),
		Ctime: time.Unix(nd.Stat.Ctim.Sec, nd.Stat.Ctim.Nsec),
		Mode: unixToFileStatMode(nd.Stat.Mode),
		Nlink: uint32(nd.Stat.Nlink),
		Uid: nd.Stat.Uid,
		Gid: nd.Stat.Gid,
		Rdev: uint32(nd.Stat.Rdev),
		BlockSize: uint32(nd.Stat.Blksize),
	}
}
