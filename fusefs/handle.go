package fusefs

import (
	"errors"
	"io"
	"os"

	"bazil.org/fuse"
	"github.com/msg555/casfs/storage"
	"github.com/msg555/casfs/unix"
)

type Handle interface {
	Read(*fuse.ReadRequest) error
	Release(*fuse.ReleaseRequest) error
}

func (conn *FuseCasfsConnection) handleReleaseRequest(req *fuse.ReleaseRequest) error {
	conn.handleLock.Lock()
	handle, ok := conn.handleMap[req.Handle]
	delete(conn.handleMap, req.Handle)
	conn.handleLock.Unlock()

	if !ok {
		return FuseError{
			source: errors.New("invalid file handle"),
			errno:  unix.EBADF,
		}
	}
	return handle.Release(req)
}

func (conn *FuseCasfsConnection) handleReadRequest(req *fuse.ReadRequest) error {
	conn.handleLock.RLock()
	handle, ok := conn.handleMap[req.Handle]
	conn.handleLock.RUnlock()

	if !ok {
		return FuseError{
			source: errors.New("invalid file handle"),
			errno:  unix.EBADF,
		}
	}
	return handle.Read(req)
}

func (conn *FuseCasfsConnection) handleFlushRequest(req *fuse.FlushRequest) error {
	// Read only file system, flush does nothing
	req.Respond()
	return nil
}

func (conn *FuseCasfsConnection) OpenHandle(handle Handle) fuse.HandleID {
	conn.handleLock.Lock()
	conn.lastHandleID++
	handleID := conn.lastHandleID
	conn.handleMap[handleID] = handle
	conn.handleLock.Unlock()
	return handleID
}

type FileHandleDir struct {
	Conn *FuseCasfsConnection
	*storage.InodeData
}

func (h *FileHandleDir) Read(req *fuse.ReadRequest) error {
	if !req.Dir {
		return unix.EISDIR
	}
	if uint64(req.Offset) == DIRENT_OFFSET_EOF {
		req.Respond(&fuse.ReadResponse{})
		return nil
	}

	buf := make([]byte, req.Size)

	lastOffset := 0
	bufOffset := 0
	complete, err := h.Conn.Server.Storage.ScanChildren(h.InodeData, uint64(req.Offset), func(offset uint64, name string, ent *storage.Dirent) bool {
		if bufOffset != 0 {
			updateDirEntryOffset(buf[lastOffset:], offset)
		}

		size := addDirEntry(buf[bufOffset:], name, ent)
		if size == 0 {
			return false
		}
		lastOffset = bufOffset
		bufOffset += size
		return true
	})
	if err != nil {
		return err
	}

	if bufOffset > 0 && complete {
		updateDirEntryOffset(buf[lastOffset:], DIRENT_OFFSET_EOF)
	}

	req.Respond(&fuse.ReadResponse{
		Data: buf[:bufOffset],
	})
	return nil
}

func (h *FileHandleDir) Release(req *fuse.ReleaseRequest) error {
	req.Respond()
	return nil
}

type FileHandleReg struct {
	*storage.InodeData
	*os.File
}

func (h *FileHandleReg) Read(req *fuse.ReadRequest) error {
	if req.Dir {
		return unix.ENOTDIR
	}

	buf := make([]byte, req.Size)
	read, err := h.File.ReadAt(buf, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}

	req.Respond(&fuse.ReadResponse{
		Data: buf[:read],
	})
	return nil
}

func (h *FileHandleReg) Release(req *fuse.ReleaseRequest) error {
	err := h.File.Close()
	if err != nil {
		return err
	}
	req.Respond()
	return nil
}
