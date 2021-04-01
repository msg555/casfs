package storage

import (
	"bytes"
	"io"
	"sort"

	"github.com/boltdb/bolt"
	"github.com/go-errors/errors"

	"github.com/msg555/casfs/btree"
	"github.com/msg555/casfs/unix"
)

type hostInode struct {
	Device uint64
	Inode  uint64
}

type dirImportContext struct {
	Storage         *StorageContext
	InodeMap        map[hostInode]*dirImportNode
	IgnoreHardlinks bool
}

type dirImportNode struct {
	Inode         *InodeData
	NodeAddress   [HASH_BYTE_LENGTH]byte
	SeenLocations []dirImportNodeLocation
}

type dirImportNodeLocation struct {
	Path      string
	EdgeIndex btree.IndexType
}

// Allocate a new inode and return a StorageNode to represent it.
func (dc *dirImportContext) CreateStorageNodeFromStat(pathHash, address, xattrAddress []byte, st *unix.Stat_t) *dirImportNode {
	nd := &dirImportNode{
		Inode:     InodeFromStat(pathHash, address, xattrAddress, st),
	}
	copy(nd.NodeAddress[:], dc.Storage.computeNodeAddress(nd.Inode))
	return nd
}

func (dc *dirImportContext) ImportSpecial(dirFd int, path string, st *unix.Stat_t) (*dirImportNode, error) {
	size := int64(0)
	addr := make([]byte, HASH_BYTE_LENGTH)
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		if st.Size > unix.PATH_MAX_LIMIT {
			return nil, errors.New("symlink path too long")
		}
		buf := make([]byte, st.Size+1)
		n, err := unix.Readlinkat(dirFd, path, buf)
		if err != nil {
			return nil, err
		} else if int64(n) != st.Size {
			return nil, errors.New("unexpected symlink data")
		}

		addr, size, err = dc.Storage.Cas.Insert(bytes.NewReader(buf[:n]))
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported special file type")
	}

	st.Size = size
	return dc.CreateStorageNodeFromStat(nil, addr, nil, st), nil
}

func (dc *dirImportContext) ImportFile(fd int, st *unix.Stat_t) (*dirImportNode, error) {
	if !unix.S_ISREG(st.Mode) {
		return nil, errors.New("must be called on regular file")
	}

	addr, size, err := dc.Storage.Cas.Insert(fdReader{FileDescriptor: fd})
	if err != nil {
		return nil, err
	}

	st.Size = size
	return dc.CreateStorageNodeFromStat(nil, addr, nil, st), nil
}

func (dc *dirImportContext) ImportDirectory(importDepth int, importPath string, fd int, st *unix.Stat_t) (*dirImportNode, error) {
	if !unix.S_ISDIR(st.Mode) {
		return nil, errors.New("must be called on directory")
	}

	children := make(map[string]*dirImportNode)

	buf := make([]byte, 1<<16)
	for {
		bytesRead, err := unix.Getdents(fd, buf)
		if err != nil {
			return nil, err
		}
		if bytesRead == 0 {
			break
		}

		for pos := 0; pos < bytesRead; {
			ino := unix.Hbo.Uint64(buf[pos:])
			// not needed
			// off := unix.Hbo.Uint64(buf[pos+8:])
			reclen := unix.Hbo.Uint16(buf[pos+16:])
			tp := uint8(buf[pos+18])
			name := NullTerminatedString(buf[pos+19 : pos+int(reclen)])
			pos += int(reclen)

			if ino == 0 {
				// Skip deleted files
				continue
			}

			if name == "." || name == ".." {
				continue
			}

			cacheInode := func(childSt *unix.Stat_t, forwardFunc func() (*dirImportNode, error)) (*dirImportNode, error) {
				if dc.IgnoreHardlinks {
					return forwardFunc()
				}

				hostInode := hostInode{
					Device: childSt.Dev,
					Inode:  childSt.Ino,
				}

				childNd, found := dc.InodeMap[hostInode]
				if !found {
					childNd, err = forwardFunc()
					if err != nil {
						return nil, err
					}
					dc.InodeMap[hostInode] = childNd
				}

				return childNd, nil
			}

			var childNd *dirImportNode
			switch tp {
			case unix.DT_FIFO, unix.DT_CHR, unix.DT_BLK, unix.DT_LNK, unix.DT_SOCK:
				var childSt unix.Stat_t
				err = unix.Fstatat(fd, name, &childSt, unix.AT_SYMLINK_NOFOLLOW)
				if err != nil {
					return nil, err
				}

				childNd, err = cacheInode(&childSt, func() (*dirImportNode, error) {
					return dc.ImportSpecial(fd, name, &childSt)
				})
				if err != nil {
					return nil, err
				}
			case unix.DT_REG, unix.DT_DIR:
				childFd, err := unix.Openat(fd, name, unix.O_RDONLY, 0)
				if err != nil {
					return nil, err
				}

				var childSt unix.Stat_t
				err = unix.Fstat(childFd, &childSt)
				if err != nil {
					unix.Close(childFd)
					return nil, err
				}

				if tp == unix.DT_DIR {
					childNd, err = dc.ImportDirectory(importDepth+1, importPath+"/"+name, childFd, &childSt)
				} else { // tp == unix.DT_REG
					childNd, err = cacheInode(&childSt, func() (*dirImportNode, error) {
						return dc.ImportFile(childFd, &childSt)
					})
				}

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

			children[name] = childNd
		}
	}

	childPaths := make([]string, 0, len(children))
	for childPath := range children {
		childPaths = append(childPaths, childPath)
	}
	sort.Strings(childPaths)

	h := dc.Storage.HashFactory()
	for _, name := range childPaths {
		childNd := children[name]

		io.WriteString(h, name)
		h.Write([]byte{0})
		h.Write(childNd.NodeAddress[:])
	}
	contentAddress := h.Sum(nil)

	h.Reset()
	io.WriteString(h, importPath)
	pathHash := h.Sum(nil)

	nd := dc.CreateStorageNodeFromStat(pathHash, contentAddress, nil, st)

	err := dc.Storage.NodeDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		v := b.Get(nd.NodeAddress[:])
		if len(v) == INODE_SIZE {
			nd.Inode = inodeFromBytes(v)
			return nil
		}

		// Construct Dirent btree
		nd.Inode.Size = 0
		direntMap := make(map[string][]byte)
		for childPath, childNd := range children {
			nd.Inode.Size += childNd.Inode.Size
			direntMap[childPath] = childNd.Inode.toBytes()
		}
		treeNode, err := dc.Storage.DirentTree.WriteRecords(direntMap)
		if err != nil {
			return err
		}

		nd.Inode.TreeNode = treeNode
		return b.Put(nd.NodeAddress[:], nd.Inode.toBytes())
	})

	if !dc.IgnoreHardlinks && err == nil {
		for childPath, childNd := range children {
			_, edgeIdx, err := dc.Storage.DirentTree.Find(nd.Inode.TreeNode, childPath)
			if err != nil {
				return nil, err
			}
			childNd.SeenLocations = append(childNd.SeenLocations, dirImportNodeLocation{
				Path:      importPath + "/" + childPath,
				EdgeIndex: edgeIdx,
			})
		}
	}

	return nd, nil
}

func (sc *StorageContext) ImportPath(pathname string) (*StorageNode, error) {
	dc := &dirImportContext{
		Storage:         sc,
		InodeMap:        make(map[hostInode]*dirImportNode),
		IgnoreHardlinks: false,
	}

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

	nd, err := dc.ImportDirectory(0, "", fd, &st)
	if err != nil {
		return nil, err
	}

	sn := &StorageNode{
		Inode: nd.Inode,
	}
	copy(sn.NodeAddress[:], nd.NodeAddress[:])

	if dc.IgnoreHardlinks {
		return sn, nil
	}
	return dc.createHardlinkLayer(sn)
}
