package storage

import (
	"golang.org/x/sys/unix"
)

const DIRENT_SIZE = 9
const MAX_PATH = 255

const (
	DT_UNKNOWN = 0
	DT_FIFO    = 1
	DT_CHR     = 2
	DT_DIR     = 4
	DT_BLK     = 6
	DT_REG     = 8
	DT_LNK     = 10
	DT_SOCK    = 12
)

type Dirent struct {
	Inode uint64
	Type  uint8
}

func fileTypeToDirentType(unixMode uint32) uint8 {
	switch unixMode & unix.S_IFMT {
	case unix.S_IFBLK:
		return DT_BLK
	case unix.S_IFCHR:
		return DT_CHR
	case unix.S_IFDIR:
		return DT_DIR
	case unix.S_IFIFO:
		return DT_FIFO
	case unix.S_IFLNK:
		return DT_LNK
	case unix.S_IFREG:
		return DT_REG
	case unix.S_IFSOCK:
		return DT_SOCK
	}
	return DT_UNKNOWN
}

func direntToBytes(ent Dirent) []byte {
	var data [9]byte
	bo.PutUint64(data[:], ent.Inode)
	data[8] = ent.Type
	return data[:]
}

func direntFromBytes(data []byte) Dirent {
	return Dirent{
		Inode: bo.Uint64(data),
		Type:  data[8],
	}
}
