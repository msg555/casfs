package storage

import (
	"encoding/binary"

	"github.com/msg555/ctrfs/btree"
	"github.com/msg555/ctrfs/unix"
)

const INODE_SIZE = 60 + 3*HASH_BYTE_LENGTH
const MODE_HARDLINK_LAYER = uint32(0xFFFFFFFF)

var bo = binary.LittleEndian

type InodeData struct {
	Mode         uint32
	Uid          uint32
	Gid          uint32
	Dev          uint64
	Atim         uint64
	Mtim         uint64
	Ctim         uint64
	Size         uint64
	TreeNode     btree.TreeIndex
	PathHash     [HASH_BYTE_LENGTH]byte
	Address      [HASH_BYTE_LENGTH]byte
	XattrAddress [HASH_BYTE_LENGTH]byte
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
	copy(buf[60:], nd.PathHash[:])
	copy(buf[60+HASH_BYTE_LENGTH:], nd.Address[:])
	if nd.Mode != MODE_HARDLINK_LAYER || !contentHash {
		copy(buf[60+2*HASH_BYTE_LENGTH:], nd.XattrAddress[:])
	} else {
		for i := 0; i < HASH_BYTE_LENGTH; i++ {
			buf[60+2*HASH_BYTE_LENGTH+i] = 0
		}
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
	copy(nd.PathHash[:], buf[60:])
	copy(nd.Address[:], buf[60+HASH_BYTE_LENGTH:])
	copy(nd.XattrAddress[:], buf[60+2*HASH_BYTE_LENGTH:])
}

func (nd *InodeData) toBytes() []byte {
	var buf [INODE_SIZE]byte
	nd.Write(buf[:], false)
	return buf[:]
}

func inodeFromBytes(buf []byte) *InodeData {
	nd := InodeData{}
	nd.Read(buf)
	return &nd
}

func InodeFromStat(pathHash, address, xattrAddress []byte, st *unix.Stat_t) *InodeData {
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
	copy(data.PathHash[:], pathHash)
	copy(data.Address[:], address)
	copy(data.XattrAddress[:], xattrAddress)
	return &data
}
