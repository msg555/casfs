package main

import (
	"github.com/msg555/casfs"
	"github.com/msg555/casfs/blockfile"
	"golang.org/x/sys/unix"
)

const INODE_SIZE = 52 + CONTENT_ADDRESS_LENGTH

type InodeData struct {
	Mode     uint32
	Uid      uint32
	Gid      uint32
	Dev      uint64
	Atim     uint64
	Mtim     uint64
	Ctim     uint64
	TreeNode blockfile.BlockIndex
	Address  [CONTENT_ADDRESS_LENGTH]byte
}

func (nd *InodeData) Write(buf []byte) {
	casfs.Hbo.PutUint32(buf[0:], nd.Mode)
	casfs.Hbo.PutUint32(buf[4:], nd.Uid)
	casfs.Hbo.PutUint32(buf[8:], nd.Gid)
	casfs.Hbo.PutUint64(buf[12:], nd.Dev)
	casfs.Hbo.PutUint64(buf[20:], nd.Atim)
	casfs.Hbo.PutUint64(buf[28:], nd.Mtim)
	casfs.Hbo.PutUint64(buf[36:], nd.Ctim)
	casfs.Hbo.PutUint64(buf[44:], nd.TreeNode)
	copy(buf[52:], nd.Address[:])
}

func (nd *InodeData) Read(buf []byte) {

}

func InodeFromStat(address []byte, st *unix.Stat_t) *InodeData {
	data := InodeData{
		Mode: st.Mode,
		Uid:  st.Uid,
		Gid:  st.Gid,
		Atim: uint64(st.Atim.Nano()),
		Mtim: uint64(st.Mtim.Nano()),
		Ctim: uint64(st.Ctim.Nano()),
	}
	if casfs.S_ISCHR(data.Mode) || casfs.S_ISBLK(data.Mode) {
		data.Dev = st.Rdev
	}
	copy(data.Address[:], address)
	return &data
}
