package storage

import (
	"encoding/binary"

	"github.com/msg555/casfs/blockfile"
	"github.com/msg555/casfs/unix"
)

const INODE_SIZE = 60 + 2*CONTENT_ADDRESS_LENGTH
const MODE_HARDLINK_LAYER = uint32(0xFFFFFFFF)

var bo = binary.LittleEndian

type InodeIndex = blockfile.BlockIndex

type InodeData struct {
	Mode         uint32
	Uid          uint32
	Gid          uint32
	Dev          uint64
	Atim         uint64
	Mtim         uint64
	Ctim         uint64
	Size         uint64
	TreeNode     blockfile.BlockIndex
	Address      [CONTENT_ADDRESS_LENGTH]byte
	XattrAddress [CONTENT_ADDRESS_LENGTH]byte
}

func (nd *InodeData) Write(buf []byte, contentHash bool) {
	bo.PutUint32(buf[0:], nd.Mode)
	bo.PutUint32(buf[4:], nd.Uid)
	bo.PutUint32(buf[8:], nd.Gid)
	bo.PutUint64(buf[12:], nd.Dev)
	bo.PutUint64(buf[20:], nd.Atim)
	bo.PutUint64(buf[28:], nd.Mtim)
	bo.PutUint64(buf[36:], nd.Ctim)
	bo.PutUint64(buf[44:], nd.Size)
	if contentHash {
		bo.PutUint64(buf[52:], 0)
	} else {
		bo.PutUint64(buf[52:], nd.TreeNode)
	}
	copy(buf[60:], nd.Address[:])
	if nd.Mode != MODE_HARDLINK_LAYER {
		copy(buf[60+CONTENT_ADDRESS_LENGTH:], nd.XattrAddress[:])
	}
}

func (nd *InodeData) Read(buf []byte) {
	nd.Mode = bo.Uint32(buf[0:])
	nd.Uid = bo.Uint32(buf[4:])
	nd.Gid = bo.Uint32(buf[8:])
	nd.Dev = bo.Uint64(buf[12:])
	nd.Atim = bo.Uint64(buf[20:])
	nd.Mtim = bo.Uint64(buf[28:])
	nd.Ctim = bo.Uint64(buf[36:])
	nd.Size = bo.Uint64(buf[44:])
	nd.TreeNode = bo.Uint64(buf[52:])
	copy(nd.Address[:], buf[60:])
	copy(nd.XattrAddress[:], buf[60+CONTENT_ADDRESS_LENGTH:])
}

func InodeFromStat(address []byte, xattrAddress []byte, st *unix.Stat_t) *InodeData {
	data := InodeData{
		Mode: st.Mode,
		Uid:  st.Uid,
		Gid:  st.Gid,
		Atim: uint64(st.Atim.Nano()),
		Mtim: uint64(st.Mtim.Nano()),
		Ctim: uint64(st.Ctim.Nano()),
		Size: uint64(st.Size),
	}
	if unix.S_ISCHR(data.Mode) || unix.S_ISBLK(data.Mode) {
		data.Dev = st.Rdev
	}
	copy(data.Address[:], address)
	copy(data.XattrAddress[:], xattrAddress)
	return &data
}
