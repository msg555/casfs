package fusefs

import (
	"time"

	"bazil.org/fuse"
	"github.com/msg555/casfs"
	"golang.org/x/sys/unix"
)

type NodeData struct {
	Cfs        *CasFS
	Node       fuse.NodeID
	Path       string
	Stat       unix.Stat_t
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


func (nd *NodeData) GetAttr() fuse.Attr {
	return fuse.Attr{
		Valid:     DURATION_DEFAULT,
		Inode:     uint64(nd.Node),
		Size:      uint64(nd.Stat.Size),
		Blocks:    uint64(nd.Stat.Blocks),
		Atime:     time.Unix(nd.Stat.Atim.Sec, nd.Stat.Atim.Nsec),
		Mtime:     time.Unix(nd.Stat.Mtim.Sec, nd.Stat.Mtim.Nsec),
		Ctime:     time.Unix(nd.Stat.Ctim.Sec, nd.Stat.Ctim.Nsec),
		Mode:      casfs.UnixToFileStatMode(nd.Stat.Mode),
		Nlink:     uint32(nd.Stat.Nlink),
		Uid:       nd.Stat.Uid,
		Gid:       nd.Stat.Gid,
		Rdev:      uint32(nd.Stat.Rdev),
		BlockSize: uint32(nd.Stat.Blksize),
	}
}
