package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os/user"
	"path"
	"sort"

	"github.com/boltdb/bolt"
	"github.com/msg555/casfs"
	"github.com/msg555/casfs/blockfile"
	"github.com/msg555/casfs/btree"
	"github.com/msg555/casfs/castore"
	"github.com/msg555/casfs/unix"
)

const AT_SYMLINK_NOFOLLOW = 0x100
const CONTENT_ADDRESS_LENGTH = 32
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
	BasePath		string
}

type StorageNode struct {
	NodeIndex blockfile.BlockIndex
	Stat      *InodeData
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
		BasePath:    basePath,
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

func (sc *StorageContext) Statfs() (*unix.Statfs_t, error) {
	var buf unix.Statfs_t
	return &buf, unix.Statfs(sc.BasePath, &buf)
}

func (sc *StorageContext) LookupAddressInode(address []byte) (blockfile.BlockIndex, error) {
	var result blockfile.BlockIndex
	err := sc.NodeDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		v := b.Get(address)
		fmt.Println("Lookup", address, v)
		if len(v) == 8 {
			result = bo.Uint64(v)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}

func (sc *StorageContext) ReadInode(nodeIndex blockfile.BlockIndex) (*InodeData, error) {
	buf, err := sc.InodeBlocks.Read(nodeIndex, nil)
	if err != nil {
		return nil, err
	}
	result := InodeData{}
	result.Read(buf)
	return &result, nil
}

func (sc *StorageContext) LookupChild(nd *InodeData, name string) (*Dirent, error) {
	data, err := sc.DirentTree.Find(nd.TreeNode, name)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	return direntFromBytes(data), nil
}

func (sc *StorageContext) ScanChildren(nd *InodeData, offset uint64, direntCallback func(offset uint64, name string, ent *Dirent) bool) (bool, error) {
	return sc.DirentTree.Scan(nd.TreeNode, offset, func(offset uint64, key string, val []byte) bool {
		return direntCallback(offset, key, direntFromBytes(val))
	})
}

// Allocate a new inode and return a StorageNode to represent it.
func (sc *StorageContext) CreateStorageNode(address []byte, st *unix.Stat_t, createCallback func(*InodeData) error) (*StorageNode, error) {
	ist := InodeFromStat(address, nil, st)
	nd := &StorageNode{
		Stat:      ist,
		NodeAddress: sc.computeNodeAddress(ist),
	}

	var nodeIndex blockfile.BlockIndex
	err := sc.NodeDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		v := b.Get(nd.NodeAddress)
		fmt.Println("Lookup", address, v)
		if len(v) == 8 {
			nodeIndex = bo.Uint64(v)
			return nil
		}
		var err error
		nodeIndex, err = sc.InodeBlocks.Allocate()
		if err != nil {
			return err
		}

		if createCallback != nil {
			err = createCallback(ist)
			if err != nil {
				return err
			}
		}

		var buf [INODE_SIZE]byte
		ist.Write(buf[:])
		err = sc.InodeBlocks.Write(nodeIndex, buf[:])
		if err != nil {
			return err
		}

		bo.PutUint64(buf[:], nodeIndex)
		return b.Put(nd.NodeAddress, buf[:8])
	})
	if err != nil {
		return nil, err
	}
	nd.NodeIndex = nodeIndex

	return nd, err
}

func (sc *StorageContext) computeNodeAddress(st *InodeData) []byte {
	h := sc.HashFactory()

	var buf [INODE_SIZE]byte
	st.Write(buf[:])
	h.Write(buf[:])

	return h.Sum(nil)
}

func NullTerminatedString(data []byte) string {
	for i, ch := range data {
		if ch == 0 {
			return string(data[:i])
		}
	}
	return string(data)
}

func (sc *StorageContext) ImportSpecial(dirFd int, path string, st *unix.Stat_t) (*StorageNode, error) {
	hostInode := HostInode{
		Device: st.Dev,
		Inode:  st.Ino,
	}
	nd, found := sc.InodeMap[hostInode]
	if found {
		return nd, nil
	}

	size := int64(0)
	addr := make([]byte, CONTENT_ADDRESS_LENGTH)
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		if st.Size > unix.PATH_MAX_LIMIT {
			return nil, errors.New("symlink path too long")
		}
		buf := make([]byte, st.Size + 1)
		n, err := unix.Readlinkat(dirFd, path, buf)
		if err != nil {
			return nil, err
		} else if int64(n) != st.Size {
			return nil, errors.New("unexpected symlink data")
		}

		addr, size, err = sc.Cas.Insert(bytes.NewReader(buf[:n]))
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported special file type")
	}

	st.Size = size
	nd, err := sc.CreateStorageNode(addr, st, nil)
	if err != nil {
		return nil, err
	}

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

	if !unix.S_ISREG(st.Mode) {
		return nil, errors.New("must be called on regular file")
	}

	addr, size, err := sc.Cas.Insert(fdReader{FileDescriptor: fd})
	if err != nil {
		return nil, err
	}

	st.Size = size
	nd, err = sc.CreateStorageNode(addr, st, nil)
	if err != nil {
		return nil, err
	}

	sc.InodeMap[hostInode] = nd
	return nd, nil
}

func (sc *StorageContext) ImportDirectory(fd int, st *unix.Stat_t) (*StorageNode, error) {
	if !unix.S_ISDIR(st.Mode) {
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
				err = unix.Fstatat(fd, path, &childSt, AT_SYMLINK_NOFOLLOW)
				if err != nil {
					return nil, err
				}

				childNd, err := sc.ImportSpecial(fd, path, &childSt)
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
		h.Write(childNd.NodeAddress)
	}

	// directory content address is hash of dirents
	// Construct Dirent...

	nd, err := sc.CreateStorageNode(h.Sum(nil), st, func(stat *InodeData) error {
		direntMap := make(map[string][]byte)
		for childPath, childNd := range children {
			println("child node", childNd.NodeIndex)
			direntMap[childPath] = direntToBytes(Dirent{
				Inode: childNd.NodeIndex,
				Type:  fileTypeToDirentType(childNd.Stat.Mode),
			})
		}
		var err error
		stat.Size = 4096 // Always just report 4096 as dir size
		stat.TreeNode, err = sc.DirentTree.WriteRecords(direntMap)
		return err
	})
	if err != nil {
		return nil, err
	}

	return nd, nil
}

func (sc *StorageContext) ImportPath(pathname string) (*StorageNode, error) {
	var st unix.Stat_t
	err := unix.Stat(pathname, &st)
	if err != nil {
		return nil, err
	}

	if !unix.S_ISDIR(st.Mode) {
		return nil, errors.New("root import path must be a directory")
	}

	fd, err := unix.Open(pathname, unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

	return sc.ImportDirectory(fd, &st)
}
