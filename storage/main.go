package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"

	"os"
	"sort"
	"syscall"
	"unsafe"

	"github.com/msg555/casfs"
)

const AT_SYMLINK_NOFOLLOW = 0x100

const (
	DT_UNKNOWN	= 0
	DT_FIFO			= 1
	DT_CHR			= 2
	DT_DIR			= 4
	DT_BLK			= 6
	DT_REG			= 8
	DT_LNK			= 10
	DT_SOCK			= 12
)

type HostInode struct {
	Device	uint64
	Inode		uint64
}

type StorageContext struct {
	Hasher		hash.Hash
	InodeMap	map[HostInode]*StorageNode
}

type StorageNode struct {
	Stat				syscall.Stat_t
	Children		map[string]*StorageNode
	ContentHash	[]byte
	NodeHash		[]byte
}

func (nd *StorageNode) ComputeNodeHash(sc *StorageContext) {
	var buf [44]byte
	casfs.Hbo.PutUint32(buf[0:], nd.Stat.Mode)
	casfs.Hbo.PutUint32(buf[4:], nd.Stat.Uid)
	casfs.Hbo.PutUint32(buf[8:], nd.Stat.Gid)
	if casfs.S_ISCHR(nd.Stat.Mode) || casfs.S_ISBLK(nd.Stat.Mode) {
		casfs.Hbo.PutUint64(buf[12:], nd.Stat.Rdev)
	}
	casfs.Hbo.PutUint64(buf[20:], uint64(nd.Stat.Atim.Nano()))
	casfs.Hbo.PutUint64(buf[28:], uint64(nd.Stat.Mtim.Nano()))
	casfs.Hbo.PutUint64(buf[36:], uint64(nd.Stat.Ctim.Nano()))

	h := sc.Hasher
	h.Reset()
	h.Write(buf[:])
	h.Write(nd.ContentHash)

	childPaths := make([]string, 0, len(nd.Children))
	for path := range nd.Children {
		childPaths = append(childPaths, path)
	}
	sort.Strings(childPaths)

	for _, path := range childPaths {
		childNd := nd.Children[path]

		io.WriteString(h, path)
		h.Write([]byte{0})
		h.Write(childNd.NodeHash)
	}

	nd.NodeHash = h.Sum(nil)
}

func Fstatat(dirfd int, pathname string, stat *syscall.Stat_t, flags int) error {
	var p *byte
	p, err := syscall.BytePtrFromString(pathname)
	if err != nil {
		return err
	}

	_, _, errno := syscall.Syscall6(syscall.SYS_NEWFSTATAT, uintptr(dirfd), uintptr(unsafe.Pointer(p)), uintptr(unsafe.Pointer(stat)), uintptr(flags), 0, 0)
	if errno != 0 {
		return errno
	}

	return nil
}

func NullTerminatedString(data []byte) string {
	for i, ch := range data {
		if ch == 0 {
			return string(data[:i])
		}
	}
	return string(data)
}

func (sc *StorageContext) ImportSpecial(st *syscall.Stat_t) (*StorageNode, error) {
	hostInode := HostInode{
		Device: st.Dev,
		Inode: st.Ino,
	}
	nd, found := sc.InodeMap[hostInode]
	if found {
		return nd, nil
	}

	h := sc.Hasher
	h.Reset()
	switch st.Mode & syscall.S_IFMT {
		case syscall.S_IFLNK:
			// readlinkat
		default:
			return nil, errors.New("unsupported special file type")
	}

	nd = &StorageNode{
		Stat: *st,
		ContentHash: h.Sum(nil),
	}
	nd.ComputeNodeHash(sc)

	sc.InodeMap[hostInode] = nd
	return nd, nil
}

func (sc *StorageContext) ImportFile(fd int, st *syscall.Stat_t) (*StorageNode, error) {
	hostInode := HostInode{
		Device: st.Dev,
		Inode: st.Ino,
	}
	nd, found := sc.InodeMap[hostInode]
	if found {
		return nd, nil
	}

	if !casfs.S_ISREG(st.Mode) {
		return nil, errors.New("must be called on regular file")
	}
	h := sc.Hasher
	h.Reset()

	buf := make([]byte, 1 << 16)
	for {
		bytesRead, err := syscall.Read(fd, buf)
		if err != nil {
			return nil, err
		}
		if bytesRead == 0 {
			break
		}
		h.Write(buf[:bytesRead])
	}

	nd = &StorageNode{
		Stat: *st,
		ContentHash: h.Sum(nil),
	}
	nd.ComputeNodeHash(sc)

	sc.InodeMap[hostInode] = nd
	return nd, nil
}

func (sc *StorageContext) ImportDirectory(fd int, st *syscall.Stat_t) (*StorageNode, error) {
	if !casfs.S_ISDIR(st.Mode) {
		return nil, errors.New("must be called on directory")
	}

	nd := StorageNode{
		Stat: *st,
		Children: make(map[string]*StorageNode),
	}

	buf := make([]byte, 1 << 16)
	for {
		bytesRead, err := syscall.Getdents(fd, buf)
		if err != nil {
			return nil, err
		}
		if bytesRead == 0 {
			break
		}
		fmt.Println("READ:", buf[:bytesRead])

		for pos := 0; pos < bytesRead; {
			ino := casfs.Hbo.Uint64(buf[pos:])
			off := casfs.Hbo.Uint64(buf[pos+8:])
			reclen := casfs.Hbo.Uint16(buf[pos+16:])
			tp := uint8(buf[pos+18])
			path := NullTerminatedString(buf[pos+19:pos+int(reclen)])
			pos += int(reclen)

			if ino == 0 {
				// Skip deleted files
				continue
			}
			fmt.Println("PATH:", len(path), path)
			fmt.Println("INO:", ino, off, reclen, tp, path)

			if path == "." || path == ".." {
				continue
			}

			switch tp {
				case DT_FIFO, DT_CHR, DT_BLK, DT_LNK, DT_SOCK:
					var childSt syscall.Stat_t
					err = Fstatat(fd, path, &childSt, AT_SYMLINK_NOFOLLOW)
					if err != nil {
						return nil, err
					}

					childNd, err := sc.ImportSpecial(&childSt)
					if err != nil {
						return nil, err
					}
					fmt.Println("Special:", path, childNd.ContentHash)
					nd.Children[path] = childNd
				case DT_REG, DT_DIR:
					childFd, err := syscall.Openat(fd, path, syscall.O_RDONLY, 0)
					if err != nil {
						return nil, err
					}

					var childSt syscall.Stat_t
					err = syscall.Fstat(childFd, &childSt)
					if err != nil {
						syscall.Close(childFd)
						return nil, err
					}

					var childNd *StorageNode
					if tp == DT_DIR {
						childNd, err = sc.ImportDirectory(childFd, &childSt)
					} else { // tp == DT_REG
						childNd, err = sc.ImportFile(childFd, &childSt)
					}
					nd.Children[path] = childNd

					if err != nil {
						syscall.Close(childFd)
						return nil, err
					}

					err = syscall.Close(childFd)
					if err != nil {
						return nil, err
					}
				default:
					return nil, errors.New("unexpected file type returned")
			}
		}
	}

	h := sc.Hasher
	h.Reset()

	childPaths := make([]string, 0, len(nd.Children))
	for path := range nd.Children {
		childPaths = append(childPaths, path)
	}
	sort.Strings(childPaths)

	for _, path := range childPaths {
		childNd := nd.Children[path]

		io.WriteString(h, path)
		h.Write([]byte{0})
		h.Write(childNd.ContentHash)
	}
	nd.ContentHash = h.Sum(nil)

	nd.ComputeNodeHash(sc)
	return &nd, nil
}

func (sc *StorageContext) ImportPath(pathname string) (*StorageNode, error) {
	var st syscall.Stat_t
	err := syscall.Stat(pathname, &st)
	if err != nil {
		return nil, err
	}

	if !casfs.S_ISDIR(st.Mode) {
		return nil, errors.New("root import path must be a directory")
	}

	fd, err := syscall.Open(pathname, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(fd)

	return sc.ImportDirectory(fd, &st)
}

func main() {
	sc := StorageContext{
		Hasher: sha256.New(),
		InodeMap: make(map[HostInode]*StorageNode),
	}
	for _, dir := range os.Args[1:] {
		_, err := sc.ImportPath(dir)
		if err != nil {
			fmt.Println("Import of", dir, "failed with", err)
		} else {
			fmt.Println("Imported", dir, "successfully")
		}
	}
}
