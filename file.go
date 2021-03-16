package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"syscall"

	"bazil.org/fuse"
)

type FileHandle struct {
	// TODO: Switch to using syscalls only for file handling.
	File				*os.File

	accessLock	sync.Mutex
}

func CreateFileHandle(nd *NodeData, flag int, perm os.FileMode) (*FileHandle, error) {
	fh, err := os.OpenFile(path.Join(nd.Cfs.OverlayDir, nd.Path), flag, perm)
	if err != nil {
		return nil, err
	}
	return &FileHandle{
		File: fh,
	}, nil
}

func CreateFileHandleFromFD(nd *NodeData, fd int, perm os.FileMode) (*FileHandle, error) {
	fh, err := os.OpenFile(path.Join(nd.Cfs.OverlayDir, nd.Path), flag, perm)
	if err != nil {
		return nil, err
	}
	return &FileHandle{
		File: fh,
	}, nil
}


func (hd *FileHandle) Release(req *fuse.ReleaseRequest) {
	fmt.Println("Release:", hd.File)
	hd.File.Close()
}

func (hd *FileHandle) Read(req *fuse.ReadRequest) {
	if req.Dir {
		req.RespondError(FuseError{
			source: errors.New("not a a directory"),
			errno:  syscall.ENOTDIR,
		})
		return
	}

	hd.accessLock.Lock()
	defer hd.accessLock.Unlock()

	buf := make([]byte, req.Size)
	totalBytesRead := 0

	for totalBytesRead < req.Size {
		bytesRead, err := hd.File.ReadAt(buf[totalBytesRead:], req.Offset+int64(totalBytesRead))

		totalBytesRead += bytesRead
		if err == io.EOF {
			return
		}
		if err != nil {
			req.RespondError(WrapIOError(err))
			return
		}
	}

	req.Respond(&fuse.ReadResponse{
		Data: buf[:totalBytesRead],
	})
}

func (hd *FileHandle) Flush(req *fuse.FlushRequest) {
	req.Respond()
}

func (hd *FileHandle) Write(req *fuse.WriteRequest) {
	hd.accessLock.Lock()
	defer hd.accessLock.Unlock()

	totalBytesWritten := 0
	for totalBytesWritten < len(req.Data) {
		bytesWritten, err := hd.File.WriteAt(req.Data[totalBytesWritten:], req.Offset + int64(totalBytesWritten))

		totalBytesWritten += bytesWritten
		if err != nil {
			fmt.Println("Error:", hd.File, err)
			req.RespondError(WrapIOError(err))
			return
		}
	}

	fmt.Println("HELLO WROTE:", totalBytesWritten)

	req.Respond(&fuse.WriteResponse{
		Size: totalBytesWritten,
	})

}
