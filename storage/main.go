package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os/user"
	"path"
	"sort"
	"unsafe"

	"github.com/boltdb/bolt"
	"github.com/msg555/casfs"
	"github.com/msg555/casfs/blockfile"
	"github.com/msg555/casfs/btree"
	"github.com/msg555/casfs/castore"
	"golang.org/x/sys/unix"
)

const AT_SYMLINK_NOFOLLOW = 0x100
const CONTENT_ADDRESS_LENGTH = 64
const INODE_BUCKET_NAME = "inodes"

type HostInode struct {
	Device uint64
	Inode  uint64
}

type StorageContext struct {
	InodeMap    map[HostInode]*StorageNode
	Cas         *castore.Castore
	HashFactory castore.HashFactory
	InodeBlocks *blockfile.BlockFile
	DirentTree  *btree.BTree
	NodeDB      *bolt.DB
}

type StorageNode struct {
	NodeIndex blockfile.BlockIndex
	Stat      *InodeData
	Children  map[string]*StorageNode
	NodeAddress  []byte
}

func OpenDefaultStorageContext() (*StorageContext, error) {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}

	return OpenStorageContext(path.Join(usr.HomeDir, ".castore"))
}

func OpenStorageContext(basePath string) (*StorageContext, error) {
	hashFactory := sha256.New
	cas, err := castore.CreateCastore(basePath, hashFactory)
	if err != nil {
		return nil, err
	}

	inodeBlocks, err := blockfile.Open(path.Join(basePath, "inodes.bin"), 0666, INODE_SIZE, false)
	if err != nil {
		return nil, err
	}

	// Choose a fan out to ensure block is under 4KB
	direntTree, err := btree.Open(path.Join(basePath, "dirent.bin"),
		0666, MAX_PATH, DIRENT_SIZE, 14, false)
	if err != nil {
		inodeBlocks.Close()
		log.Fatal(err)
	}

	nodeDB, err := bolt.Open(path.Join(basePath, "contentmap.db"), 0666, nil)
	if err != nil {
		direntTree.Close()
		inodeBlocks.Close()
		log.Fatal(err)
	}

	err = nodeDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(INODE_BUCKET_NAME))
		return err
	})
	if err != nil {
		nodeDB.Close()
		direntTree.Close()
		inodeBlocks.Close()
		log.Fatal(err)
	}

	return &StorageContext{
		InodeMap:    make(map[HostInode]*StorageNode),
		HashFactory: hashFactory,
		Cas:         cas,
		InodeBlocks: inodeBlocks,
		DirentTree:  direntTree,
		NodeDB:      nodeDB,
	}, nil
}

func (sc *StorageContext) Close() error {
	if sc.InodeBlocks != nil {
		err := sc.InodeBlocks.Close()
		if err != nil {
			return err
		}
	}
	if sc.DirentTree != nil {
		err := sc.DirentTree.Close()
		if err != nil {
			return err
		}
	}
	if sc.NodeDB != nil {
		err := sc.NodeDB.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *StorageContext) LookupAddressInode(address []byte) (blockfile.BlockIndex, error) {
	var result blockfile.BlockIndex
	err := sc.NodeDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		v := b.Get(address)
		if len(v) == 8 {
			result = casfs.Hbo.Uint64(v)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}

// Allocate a new inode and return a StorageNode to represent it.
func (sc *StorageContext) CreateStorageNode(address []byte, st *unix.Stat_t, createCallback func(*InodeData) error) (*StorageNode, error) {
	stat := InodeFromStat(address, nil, st)

	var nodeIndex blockfile.BlockIndex
	err := sc.NodeDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		v := b.Get(address)
		if len(v) == 8 {
			nodeIndex = casfs.Hbo.Uint64(v)
			return nil
		}
		nodeIndex, err := sc.InodeBlocks.Allocate()
		if err != nil {
			return err
		}

		if createCallback != nil {
			err = createCallback(stat)
			if err != nil {
				return err
			}
		}

		var buf [INODE_SIZE]byte
		stat.Write(buf[:])
		err = sc.InodeBlocks.Write(nodeIndex, buf[:])
		if err != nil {
			return err
		}

		casfs.Hbo.PutUint64(buf[:], nodeIndex)
		return b.Put(address, buf[:8])
	})
	if err != nil {
		return nil, err
	}

	return &StorageNode{
		NodeIndex: nodeIndex,
		Stat:      stat,
	}, nil
}

func (nd *StorageNode) ComputeNodeAddress(sc *StorageContext) {
	h := sc.HashFactory()

	var buf [INODE_SIZE]byte
	nd.Stat.Write(buf[:])
	h.Write(buf[:])

	nd.NodeAddress = h.Sum(nil)
}

func Fstatat(dirfd int, pathname string, stat *unix.Stat_t, flags int) error {
	var p *byte
	p, err := unix.BytePtrFromString(pathname)
	if err != nil {
		return err
	}

	_, _, errno := unix.Syscall6(unix.SYS_NEWFSTATAT, uintptr(dirfd), uintptr(unsafe.Pointer(p)), uintptr(unsafe.Pointer(stat)), uintptr(flags), 0, 0)
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

func (sc *StorageContext) ImportSpecial(st *unix.Stat_t) (*StorageNode, error) {
	hostInode := HostInode{
		Device: st.Dev,
		Inode:  st.Ino,
	}
	nd, found := sc.InodeMap[hostInode]
	if found {
		return nd, nil
	}

	h := sc.HashFactory()
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		// readlinkat
	default:
		return nil, errors.New("unsupported special file type")
	}

	nd = &StorageNode{
		Stat: InodeFromStat(h.Sum(nil), nil, st),
	}
	nd.ComputeNodeAddress(sc)

	sc.InodeMap[hostInode] = nd
	return nd, nil
}

type fdReader struct {
	FileDescriptor int
}

func (f fdReader) Read(buf []byte) (int, error) {
	n, err := unix.Read(f.FileDescriptor, buf)
	if err == nil && n == 0 {
		return 0, io.EOF
	}
	return n, err
}

func (sc *StorageContext) ImportFile(fd int, st *unix.Stat_t) (*StorageNode, error) {
	hostInode := HostInode{
		Device: st.Dev,
		Inode:  st.Ino,
	}
	nd, found := sc.InodeMap[hostInode]
	if found {
		return nd, nil
	}

	if !casfs.S_ISREG(st.Mode) {
		return nil, errors.New("must be called on regular file")
	}

	addr, err := sc.Cas.Insert(fdReader{FileDescriptor: fd})
	if err != nil {
		return nil, err
	}

	nd, err = sc.CreateStorageNode(addr, st, nil)
	if err != nil {
		return nil, err
	}
	nd.ComputeNodeAddress(sc)

	sc.InodeMap[hostInode] = nd
	return nd, nil
}

func (sc *StorageContext) ImportDirectory(fd int, st *unix.Stat_t) (*StorageNode, error) {
	if !casfs.S_ISDIR(st.Mode) {
		return nil, errors.New("must be called on directory")
	}

	children := make(map[string]*StorageNode)

	buf := make([]byte, 1<<16)
	for {
		bytesRead, err := unix.Getdents(fd, buf)
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
			path := NullTerminatedString(buf[pos+19 : pos+int(reclen)])
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
				var childSt unix.Stat_t
				err = Fstatat(fd, path, &childSt, AT_SYMLINK_NOFOLLOW)
				if err != nil {
					return nil, err
				}

				childNd, err := sc.ImportSpecial(&childSt)
				if err != nil {
					return nil, err
				}
				children[path] = childNd
			case DT_REG, DT_DIR:
				childFd, err := unix.Openat(fd, path, unix.O_RDONLY, 0)
				if err != nil {
					return nil, err
				}

				var childSt unix.Stat_t
				err = unix.Fstat(childFd, &childSt)
				if err != nil {
					unix.Close(childFd)
					return nil, err
				}

				var childNd *StorageNode
				if tp == DT_DIR {
					childNd, err = sc.ImportDirectory(childFd, &childSt)
				} else { // tp == DT_REG
					childNd, err = sc.ImportFile(childFd, &childSt)
				}
				children[path] = childNd

				if err != nil {
					unix.Close(childFd)
					return nil, err
				}

				err = unix.Close(childFd)
				if err != nil {
					return nil, err
				}
			default:
				return nil, errors.New("unexpected file type returned")
			}
		}
	}

	h := sc.HashFactory()

	childPaths := make([]string, 0, len(children))
	for childPath := range children {
		childPaths = append(childPaths, childPath)
	}
	sort.Strings(childPaths)

	for _, path := range childPaths {
		childNd := children[path]

		io.WriteString(h, path)
		h.Write([]byte{0})
		h.Write(childNd.Stat.Address[:])
	}

	// directory content address is hash of dirents
	// Construct Dirent...

	nd, err := sc.CreateStorageNode(h.Sum(nil), st, func(stat *InodeData) error {
		direntMap := make(map[string][]byte)
		for childPath, childNd := range children {
			direntMap[childPath] = direntToBytes(Dirent{
				Inode: childNd.NodeIndex,
				Type:  fileTypeToDirentType(childNd.Stat.Mode),
			})
		}
		var err error
		stat.TreeNode, err = sc.DirentTree.WriteRecords(direntMap)
		return err
	})
	if err != nil {
		return nil, err
	}

	nd.ComputeNodeAddress(sc)
	return nd, nil
}

func (sc *StorageContext) ImportPath(pathname string) (*StorageNode, error) {
	var st unix.Stat_t
	err := unix.Stat(pathname, &st)
	if err != nil {
		return nil, err
	}

	if !casfs.S_ISDIR(st.Mode) {
		return nil, errors.New("root import path must be a directory")
	}

	fd, err := unix.Open(pathname, unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

	return sc.ImportDirectory(fd, &st)
}
