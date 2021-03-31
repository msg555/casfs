package storage

import (
	"bytes"
	"errors"
	"io"
	"sort"

	"github.com/msg555/casfs/blockfile"
	"github.com/msg555/casfs/btree"
	"github.com/msg555/casfs/unix"
)

type dirImportContext struct {
	Storage         *StorageContext
	InodeMap        map[HostInode]*dirImportNode
	IgnoreHardlinks bool
}

type dirImportNode struct {
	NodeIndex     blockfile.BlockIndex
	Stat          *InodeData
	NodeAddress   [CONTENT_ADDRESS_LENGTH]byte
	SeenLocations []dirImportNodeLocation
}

type dirImportNodeLocation struct {
	Path      string
	EdgeIndex btree.IndexType
}

// Allocate a new inode and return a StorageNode to represent it.
func (dc *dirImportContext) CreateStorageNodeFromStat(address []byte, st *unix.Stat_t, createCallback func(*InodeData) error) (*dirImportNode, error) {
	nodeIndex, err := dc.Storage.InodeBlocks.Allocate()
	if err != nil {
		return nil, err
	}

	ist := InodeFromStat(address, nil, st)
	nd := &dirImportNode{
		Stat:      ist,
		NodeIndex: nodeIndex,
	}
	copy(nd.NodeAddress[:], dc.Storage.computeNodeAddress(ist))

	if createCallback != nil {
		err = createCallback(ist)
		if err != nil {
			return nil, err
		}
	}

	var buf [INODE_SIZE]byte
	ist.Write(buf[:], false)
	err = dc.Storage.InodeBlocks.Write(nodeIndex, buf[:])
	if err != nil {
		return nil, err
	}

	return nd, err
}

func (dc *dirImportContext) ImportSpecial(dirFd int, path string, st *unix.Stat_t) (*dirImportNode, error) {
	size := int64(0)
	addr := make([]byte, CONTENT_ADDRESS_LENGTH)
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
	nd, err := dc.CreateStorageNodeFromStat(addr, st, nil)
	if err != nil {
		return nil, err
	}

	return nd, nil
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
	nd, err := dc.CreateStorageNodeFromStat(addr, st, nil)
	if err != nil {
		return nil, err
	}

	return nd, nil
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

				hostInode := HostInode{
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
			case DT_FIFO, DT_CHR, DT_BLK, DT_LNK, DT_SOCK:
				var childSt unix.Stat_t
				err = unix.Fstatat(fd, name, &childSt, AT_SYMLINK_NOFOLLOW)
				if err != nil {
					return nil, err
				}

				childNd, err = cacheInode(&childSt, func() (*dirImportNode, error) {
					return dc.ImportSpecial(fd, name, &childSt)
				})
				if err != nil {
					return nil, err
				}
			case DT_REG, DT_DIR:
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

				if tp == DT_DIR {
					childNd, err = dc.ImportDirectory(importDepth+1, importPath+"/"+name, childFd, &childSt)
				} else { // tp == DT_REG
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

	nd, err := dc.CreateStorageNodeFromStat(h.Sum(nil), st, func(stat *InodeData) error {
		// Construct Dirent btree
		stat.Size = 0
		direntMap := make(map[string][]byte)
		for childPath, childNd := range children {
			stat.Size += childNd.Stat.Size
			direntMap[childPath] = direntToBytes(Dirent{
				Inode: childNd.NodeIndex,
				Type:  fileTypeToDirentType(childNd.Stat.Mode),
			})
		}

		var err error
		stat.TreeNode, err = dc.Storage.DirentTree.WriteRecords(direntMap)
		if !dc.IgnoreHardlinks && err == nil {
			for childPath, childNd := range children {
				_, edgeIdx, err := dc.Storage.DirentTree.Find(stat.TreeNode, childPath)
				if err != nil {
					return err
				}
				childNd.SeenLocations = append(childNd.SeenLocations, dirImportNodeLocation{
					Path:      importPath + "/" + childPath,
					EdgeIndex: edgeIdx,
				})
			}
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	return nd, nil
}

func (dc *dirImportContext) CreateHardlinkLayer(rootNode *dirImportNode) (*StorageNode, error) {
	dc.Storage.InodeMap.Init()
	var linkedNodes [][]dirImportNodeLocation
	for _, nd := range dc.InodeMap {
		if len(nd.SeenLocations) <= 1 {
			continue
		}
		sort.Slice(nd.SeenLocations, func(i, j int) bool {
			return nd.SeenLocations[i].Path < nd.SeenLocations[j].Path
		})
		for _, loc := range nd.SeenLocations[1:] {
			dc.Storage.InodeMap.Map[loc.EdgeIndex] = nd.SeenLocations[0].EdgeIndex
		}
		linkedNodes = append(linkedNodes, nd.SeenLocations)
	}
	sort.Slice(linkedNodes, func(i, j int) bool {
		return linkedNodes[i][0].Path < linkedNodes[j][0].Path
	})

	h := dc.Storage.HashFactory()
	h.Write(rootNode.NodeAddress[:])

	var buf [4]byte
	bo.PutUint32(buf[:], uint32(len(linkedNodes)))
	_, err := h.Write(buf[:])
	if err != nil {
		return nil, err
	}
	for _, linkedNode := range linkedNodes {
		bo.PutUint32(buf[:], uint32(len(linkedNode)))
		_, err := h.Write(buf[:])
		if err != nil {
			return nil, err
		}
		for _, loc := range linkedNode {
			io.WriteString(h, loc.Path)
			h.Write([]byte{0})
		}
	}
	nodeAddress := h.Sum(nil)

	pr, pw := io.Pipe()
	go func() {
		dc.Storage.InodeMap.Write(pw)
		pw.Close()
	}()
	hlMapAddress, _, err := dc.Storage.Cas.Insert(pr)
	if err != nil {
		return nil, err
	}

	inode := &InodeData{
		Mode:     MODE_HARDLINK_LAYER,
		TreeNode: rootNode.NodeIndex,
	}
	copy(inode.Address[:], nodeAddress)
	copy(inode.XattrAddress[:], hlMapAddress)

	sn := &StorageNode{
		NodeIndex:   rootNode.NodeIndex,
		Stat:        inode,
		NodeAddress: inode.Address,
	}
	return sn, nil
}

func (sc *StorageContext) ImportPath(pathname string) (*StorageNode, error) {
	dc := &dirImportContext{
		Storage:         sc,
		InodeMap:        make(map[HostInode]*dirImportNode),
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

	if !dc.IgnoreHardlinks {
		return dc.CreateHardlinkLayer(nd)
	}
	return &StorageNode{
		NodeIndex:   nd.NodeIndex,
		Stat:        nd.Stat,
		NodeAddress: nd.NodeAddress,
	}, nil
}
