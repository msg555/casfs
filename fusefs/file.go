package fusefs

import (
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"syscall"

	"bazil.org/fuse"
)

type FileHandle struct {
	FileDescriptor	int
	OpenFlags				int

	accessLock			sync.Mutex
}

func CreateFileHandle(nd *NodeData, flags int) (*FileHandle, error) {
	fd, err := syscall.Open(path.Join(nd.Cfs.OverlayDir, nd.Path), flags, 0)
	if err != nil {
		return nil, err
	}
	return &FileHandle{
		FileDescriptor: fd,
		OpenFlags: flags,
	}, nil
}

func CreateFileHandleFromFD(nd *NodeData, fd int, flags int) *FileHandle {
	return &FileHandle{
		FileDescriptor: fd,
		OpenFlags: flags,
	}
}


func (hd *FileHandle) Release() error {
	return syscall.Close(hd.FileDescriptor)
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
		bytesRead, err := syscall.Pread(hd.FileDescriptor, buf[totalBytesRead:], req.Offset+int64(totalBytesRead))

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
		bytesWritten, err := syscall.Pwrite(hd.FileDescriptor, req.Data[totalBytesWritten:], req.Offset + int64(totalBytesWritten))

		totalBytesWritten += bytesWritten
		if err != nil {
			fmt.Println("Error:", hd.FileDescriptor, err)
			req.RespondError(WrapIOError(err))
			return
		}
	}

	fmt.Println("HELLO WROTE:", totalBytesWritten)

	req.Respond(&fuse.WriteResponse{
		Size: totalBytesWritten,
	})

}
