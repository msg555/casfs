package main

import (
	"errors"
	"fmt"
	"syscall"

	"bazil.org/fuse"
)

const MAX_BUFFER int = 4096

/*
type UnixDirent struct {
	uint32		d_ino
	uint32		d_off
	uint16		d_reclen
	
}
*/

type HandleData interface {
	Release(*fuse.ReleaseRequest)
	Read(*fuse.ReadRequest)
}

type DirectoryHandle struct {
	FileDescriptor	int
}

func (hd *DirectoryHandle) Release(req *fuse.ReleaseRequest) {
	syscall.Close(hd.FileDescriptor)
}

/*
TODO: Look at fill_dir at https://github.com/libfuse/libfuse/blob/master/lib/fuse.c
to see how to add entries to the directory listing buffer.

Also see
https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details
for further notes about how fill_dir is meant to be used.
*/
func fillDir(, name string, stat syscall.Stat_t, off uint32, fill_flags)


func (hd *DirectoryHandle) Read(req *fuse.ReadRequest) {
	if !req.Dir {
		req.RespondError(FuseError{
			source: errors.New("is a directory"),
			errno: syscall.EISDIR,
		})
		return
	}
	fmt.Println(req)
	fmt.Println("Offset:", req.Offset)
	fmt.Println("HELLO")

	bufSize := req.Size
	if bufSize > MAX_BUFFER {
		bufSize = MAX_BUFFER
	}
	buf := make([]byte, bufSize)

	n, err := syscall.ReadDirent(hd.FileDescriptor, buf)
	if err != nil {
		req.RespondError(WrapIOError(err))
		return

	}

	result := make([]byte, n)
	copy(result, buf[0:n])
	fmt.Println(n, err, result)

	req.Respond(&fuse.ReadResponse{
		Data: result,
	})
}
