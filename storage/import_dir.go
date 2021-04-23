package storage

import (
	"bytes"

	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/unix"
)

type hostInode struct {
	Device uint64
	Inode  uint64
}

type dirImportContext struct {
	Storage         *StorageContext
	InodeMap        map[hostInode]*importNode
	IgnoreHardlinks bool
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

func nullTerminatedString(data []byte) string {
  for i, ch := range data {
    if ch == 0 {
      return string(data[:i])
    }
  }
  return string(data)
}

// Allocate a new inode and return a StorageNode to represent it.
func (dc *dirImportContext) CreateStorageNodeFromStat(pathHash, address, xattrAddress []byte, st *unix.Stat_t) *importNode {
	nd := &importNode{
		Inode: InodeFromStat(st),
	}
	copy(nd.NodeAddress[:], dc.Storage.computeNodeAddress(nd.Inode))
	return nd
}

func (dc *dirImportContext) ImportFile(fd int, st *unix.Stat_t) (*importNode, error) {
	size := int64(0)
	addr := make([]byte, HASH_BYTE_LENGTH)
	var err error
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		addr, size, err = dc.Storage.Cas.Insert(fdReader{FileDescriptor: fd})
		if err != nil {
			return nil, err
		}
	case unix.S_IFLNK:
		if st.Size > unix.PATH_MAX_LIMIT {
			return nil, errors.New("symlink path too long")
		}
		buf := make([]byte, st.Size+1)
		n, err := unix.Readlinkat(fd, "", buf)
		if err != nil {
			return nil, err
		} else if int64(n) != st.Size {
			return nil, errors.New("unexpected symlink data")
		}

		addr, size, err = dc.Storage.Cas.Insert(bytes.NewReader(buf[:n]))
		if err != nil {
			return nil, err
		}
	case unix.S_IFBLK, unix.S_IFCHR, unix.S_IFIFO, unix.S_IFSOCK:
	default:
		return nil, errors.New("unsupported special file type")
	}

	st.Size = size
	return dc.CreateStorageNodeFromStat(nil, addr, nil, st), nil
}

func (dc *dirImportContext) ImportDirectory(importDepth int, importPath string, fd int, st *unix.Stat_t) (*importNode, error) {
	if !unix.S_ISDIR(st.Mode) {
		return nil, errors.New("must be called on directory")
	}

	children := make(map[string]*importNode)

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

			if !validatePathName(name) {
				continue
			}

			cacheInode := func(childSt *unix.Stat_t, forwardFunc func() (*importNode, error)) (*importNode, error) {
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

			flags := unix.O_PATH | unix.O_NOFOLLOW
			if tp == unix.DT_REG || tp == unix.DT_DIR {
				flags = unix.O_RDONLY | unix.O_NOFOLLOW
			}
			childFd, err := unix.Openat(fd, name, flags, 0)
			if err != nil {
				return nil, err
			}

			var childSt unix.Stat_t
			err = unix.Fstat(childFd, &childSt)
			if err != nil {
				unix.Close(childFd)
				return nil, err
			}

			var childNd *importNode
			switch tp {
			case unix.DT_FIFO, unix.DT_CHR, unix.DT_BLK, unix.DT_LNK, unix.DT_SOCK, unix.DT_REG:
				childNd, err = cacheInode(&childSt, func() (*importNode, error) {
					return dc.ImportFile(childFd, &childSt)
				})
				if err != nil {
					unix.Close(childFd)
					return nil, err
				}
			case unix.DT_DIR:
				childNd, err = dc.ImportDirectory(importDepth+1, importPath+"/"+name, childFd, &childSt)
				if err != nil {
					unix.Close(childFd)
					return nil, err
				}
			default:
				return nil, errors.New("unexpected file type returned")
			}

			err = unix.Close(childFd)
			if err != nil {
				return nil, err
			}

			children[name] = childNd
		}
	}

	nd := dc.CreateStorageNodeFromStat(nil, nil, nil, st)
	err := dc.Storage.createDirentTree(nd, importPath, children, dc.IgnoreHardlinks)
	if err != nil {
		return nd, nil
	}

	return nd, nil
}

func (sc *StorageContext) ImportPath(pathname string) (*StorageNode, error) {
	dc := &dirImportContext{
		Storage:         sc,
		InodeMap:        make(map[hostInode]*importNode),
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
	return sc.createHardlinkLayer(sn, dc.InodeMap)
}
