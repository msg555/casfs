package main

import (
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
)

type NodeData struct {
	Cfs        *CasFS
	Node       fuse.NodeID
	Path       string
	Stat       syscall.Stat_t
	ParentNode fuse.NodeID
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

func UnixToFileStatMode(unixMode uint32) os.FileMode {
	fsMode := os.FileMode(unixMode & 0777)
	switch unixMode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		fsMode |= os.ModeDevice
	case syscall.S_IFCHR:
		fsMode |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		fsMode |= os.ModeDir
	case syscall.S_IFIFO:
		fsMode |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		fsMode |= os.ModeSymlink
	case syscall.S_IFREG:
		// nothing to do
	case syscall.S_IFSOCK:
		fsMode |= os.ModeSocket
	}
	if (unixMode & syscall.S_ISGID) != 0 {
		fsMode |= os.ModeSetgid
	}
	if (unixMode & syscall.S_ISUID) != 0 {
		fsMode |= os.ModeSetuid
	}
	if (unixMode & syscall.S_ISVTX) != 0 {
		fsMode |= os.ModeSticky
	}
	return fsMode
}

func FileStatToUnixMode(fsMode os.FileMode) uint32 {
	unixMode := uint32(fsMode & 0777)
	if (fsMode & os.ModeCharDevice) != 0 {
		unixMode |= syscall.S_IFCHR
	} else if (fsMode & os.ModeDevice) != 0 {
		unixMode |= syscall.S_IFBLK
	} else if (fsMode & os.ModeDir) != 0 {
		unixMode |= syscall.S_IFDIR
	} else if (fsMode & os.ModeNamedPipe) != 0 {
		unixMode |= syscall.S_IFIFO
	} else if (fsMode & os.ModeSymlink) != 0 {
		unixMode |= syscall.S_IFLNK
	} else if (fsMode & os.ModeSocket) != 0 {
		unixMode |= syscall.S_IFSOCK
	} else {
		unixMode |= syscall.S_IFREG
	}
	if (fsMode & os.ModeSetgid) != 0 {
		unixMode |= syscall.S_ISGID
	}
	if (fsMode & os.ModeSetuid) != 0 {
		unixMode |= syscall.S_ISUID
	}
	if (fsMode & os.ModeSticky) != 0 {
		unixMode |= syscall.S_ISVTX
	}
	return unixMode
}

func (nd *NodeData) GetAttr() fuse.Attr {
	return fuse.Attr{
		Valid:     DURATION_DEFAULT,
		Inode:     uint64(nd.Node),
		Size:      uint64(nd.Stat.Size),
		Blocks:    uint64(nd.Stat.Blocks),
		Atime:     time.Unix(nd.Stat.Atim.Sec, nd.Stat.Atim.Nsec),
		Mtime:     time.Unix(nd.Stat.Mtim.Sec, nd.Stat.Mtim.Nsec),
		Ctime:     time.Unix(nd.Stat.Ctim.Sec, nd.Stat.Ctim.Nsec),
		Mode:      UnixToFileStatMode(nd.Stat.Mode),
		Nlink:     uint32(nd.Stat.Nlink),
		Uid:       nd.Stat.Uid,
		Gid:       nd.Stat.Gid,
		Rdev:      uint32(nd.Stat.Rdev),
		BlockSize: uint32(nd.Stat.Blksize),
	}
}
