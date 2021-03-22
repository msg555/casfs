package fusefs

import (
	"errors"
	"io"
	"os"
	"path"

	"bazil.org/fuse"
	"github.com/msg555/casfs"
	"golang.org/x/sys/unix"
)

const MAX_BUFFER int = 4096

type HandleData interface {
	Flush(*fuse.FlushRequest)
	Read(*fuse.ReadRequest)
	Write(*fuse.WriteRequest)

	Release() error
}

type DirectoryHandle struct {
	File    *os.File
	Entries []os.DirEntry
}

func CreateDirectoryHandle(nd *NodeData) (*DirectoryHandle, error) {
	fh, err := os.Open(path.Join(nd.Cfs.OverlayDir, nd.Path))
	if err != nil {
		return nil, err
	}
	return &DirectoryHandle{
		File: fh,
	}, nil
}

func (hd *DirectoryHandle) Release() error {
	return hd.File.Close()
}

/*
TODO: Look at fill_dir at https://github.com/libfuse/libfuse/blob/master/lib/fuse.c
to see how to add entries to the directory listing buffer.

Also see
https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details
for further notes about how fill_dir is meant to be used.

https://libfuse.github.io/doxygen/fuse__lowlevel_8h.html#ad1957bcc8ece8c90f16c42c4daf3053f
*/
func direntAlign(x int) int {
	return (x + 7) &^ 7
}

func addDirEntry(buf []byte, name string, stat *unix.Stat_t) int {
	/*
			define FUSE_DIRENT_ALIGN(x) (((x) + sizeof(__u64) - 1) & ~(sizeof(__u64) - 1))

			struct fuse_dirent {
				u64   ino;
				u64   off;
				u32   namelen;
				u32   type;
		    char name[];
		  };
	*/

	entryBaseLen := 24 + len(name)
	entryPadLen := direntAlign(entryBaseLen)
	if len(buf) < entryPadLen {
		return 0
	}

	if stat == nil {
		casfs.Hbo.PutUint64(buf[0:], uint64(FUSE_UNKNOWN_INO))
		casfs.Hbo.PutUint64(buf[8:], 0)
		casfs.Hbo.PutUint32(buf[16:], uint32(len(name)))
		casfs.Hbo.PutUint32(buf[20:], 0)
	} else {
		casfs.Hbo.PutUint64(buf[0:], uint64(FUSE_UNKNOWN_INO))
		casfs.Hbo.PutUint64(buf[8:], 0)
		casfs.Hbo.PutUint32(buf[16:], uint32(len(name)))
		casfs.Hbo.PutUint32(buf[20:], 0)
	}
	copy(buf[24:], name)
	for i := entryBaseLen; i < entryPadLen; i++ {
		buf[i] = 0
	}

	return entryPadLen
}

func (hd *DirectoryHandle) Read(req *fuse.ReadRequest) {
	if !req.Dir {
		req.RespondError(FuseError{
			source: errors.New("is a directory"),
			errno:  unix.EISDIR,
		})
		return
	}

	var err error

	bufOffset := 0
	buf := make([]byte, req.Size)
	for {
		if len(hd.Entries) == 0 {
			hd.Entries, err = hd.File.ReadDir(32)
			if err != nil {
				if err == io.EOF {
					break
				}
				req.RespondError(WrapIOError(err))
				return
			}
		}

		size := addDirEntry(buf[bufOffset:], hd.Entries[0].Name(), nil)
		if size == 0 {
			break
		}
		bufOffset += size
		hd.Entries = hd.Entries[1:]
	}

	req.Respond(&fuse.ReadResponse{
		Data: buf[:bufOffset],
	})
}

func (hd *DirectoryHandle) Flush(req *fuse.FlushRequest) {
	req.Respond()
}

func (hd *DirectoryHandle) Write(req *fuse.WriteRequest) {
	req.RespondError(FuseError{
		source: errors.New("cannot write to a directory"),
		errno:  unix.EIO,
	})
}
