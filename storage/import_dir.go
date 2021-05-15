package storage

import (
	"io"

	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/unix"
)

type hostInode struct {
	Device uint64
	Inode  uint64
}

type dirImportContext struct {
	Storage         *StorageContext
	MountView *MountView
	HostInodeMap        map[hostInode]InodeId
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

func (dc *dirImportContext) ImportFile(fd int, st *unix.Stat_t) (InodeId, error) {
	inodeData := InodeFromStat(st)
	inodeData.Size = 0

	file, err := dc.MountView.FileManager.NewFile(inodeData)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	switch st.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		written, err := io.Copy(file.(io.Writer), &fdReader{FileDescriptor: fd})
		if err != nil {
			return 0, err
		}
		if written != st.Size {
			return 0, errors.New("failed to copy file")
		}
	case unix.S_IFLNK:
		if st.Size > unix.PATH_MAX_LIMIT {
			return 0, errors.New("symlink path too long")
		}
		buf := make([]byte, st.Size+1)
		n, err := unix.Readlinkat(fd, "", buf)
		if err != nil {
			return 0, err
		} else if int64(n) != st.Size {
			return 0, errors.New("unexpected symlink data")
		}

		written, err := file.(io.Writer).Write(buf[:n])
		if err != nil {
			return 0, err
		}
		if int64(written) != st.Size {
			return 0, errors.New("failed to write symlink")
		}
	}

	return file.GetInodeId(), nil
}

func (dc *dirImportContext) ImportDirectory(importDepth int, importPath string, fd int, st *unix.Stat_t) (InodeId, error) {
	if !unix.S_ISDIR(st.Mode) {
		return 0, errors.New("must be called on directory")
	}

	inodeData := InodeFromStat(st)
	file, err := dc.MountView.FileManager.NewFile(inodeData)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	fileDir := file.(FileObjectDir)

	buf := dc.Storage.Cache.Pool.Get().([]byte)
	defer dc.Storage.Cache.Pool.Put(buf)
	for {
		bytesRead, err := unix.Getdents(fd, buf)
		if err != nil {
			return 0, err
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
			name := nullTerminatedString(buf[pos+19 : pos+int(reclen)])
			pos += int(reclen)

			if ino == 0 {
				// Skip deleted files
				continue
			}
			if !validatePathName(name) {
				continue
			}

			flags := unix.O_PATH | unix.O_NOFOLLOW
			if tp == unix.DT_REG || tp == unix.DT_DIR {
				flags = unix.O_RDONLY | unix.O_NOFOLLOW
			}
			childFd, err := unix.Openat(fd, name, flags, 0)
			if err != nil {
				return 0, err
			}

			var childSt unix.Stat_t
			err = unix.Fstat(childFd, &childSt)
			if err != nil {
				unix.Close(childFd)
				return 0, err
			}

			err = nil
			var childInodeId InodeId
			switch tp {
			case unix.DT_FIFO, unix.DT_CHR, unix.DT_BLK, unix.DT_LNK, unix.DT_SOCK, unix.DT_REG:
				hostInode := hostInode{
					Device: childSt.Dev,
					Inode:  childSt.Ino,
				}
				childInodeId, found := dc.HostInodeMap[hostInode]

				if !found {
					childInodeId, err = dc.ImportFile(childFd, &childSt)
					if err == nil {
						dc.HostInodeMap[hostInode] = childInodeId
					}
				}
			case unix.DT_DIR:
				childInodeId, err = dc.ImportDirectory(importDepth+1, importPath+"/"+name, childFd, &childSt)
			default:
				err = errors.New("unexpected file type returned")
			}
			if err != nil {
				unix.Close(childFd)
				return 0, err
			}
			if err := unix.Close(childFd); err != nil {
				return 0, err
			}
			if err := fileDir.Link(name, int(tp), childInodeId, false); err != nil {
				return 0, err
			}
		}
	}

	return file.GetInodeId(), nil
}

func (sc *StorageContext) ImportPath(pathname string) (*StorageNode, error) {
	mountView, err := sc.CreateEmptyMount()
	if err != nil {
		return nil, err
	}

	dc := &dirImportContext{
		Storage:         sc,
		MountView: mountView,
		HostInodeMap:        make(map[hostInode]InodeId),
		IgnoreHardlinks: false,
	}

	var st unix.Stat_t
	if err := unix.Stat(pathname, &st); err != nil {
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

	inodeId, err := dc.ImportDirectory(0, "", fd, &st)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
