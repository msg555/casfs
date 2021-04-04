package fusefs

import (
	"io"
	"log"
	"path/filepath"
	"sync"
	"time"

	"bazil.org/fuse"
	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/storage"
)

const DURATION_DEFAULT time.Duration = time.Duration(1000000000 * 60 * 60)

type Server struct {
	Storage *storage.StorageContext

	closing       bool
	mountLock     *sync.Mutex
	mountCond     *sync.Cond
	connectionMap map[string]*Connection
}

func CreateServerWithStorage(sc *storage.StorageContext) *Server {
	lck := &sync.Mutex{}
	return &Server{
		Storage:       sc,
		connectionMap: make(map[string]*Connection),
		mountLock:     lck,
		mountCond:     sync.NewCond(lck),
	}
}

func CreateDefaultServer() (*Server, error) {
	sc, err := storage.OpenDefaultStorageContext()
	if err != nil {
		return nil, err
	}
	return CreateServerWithStorage(sc), nil
}

func CreateServer(basePath string) (*Server, error) {
	sc, err := storage.OpenStorageContext(basePath)
	if err != nil {
		return nil, err
	}
	return CreateServerWithStorage(sc), nil
}

// Unmounts all mounts that were created under this server and waits for their
// connections to close. The releases all storage resources. If unmount fails
// on any mount this may exit with a non-nil error without releasing all held
// resources. Close() blocks until all underlying connections exit.
func (srv *Server) Close() error {
	srv.mountLock.Lock()
	srv.closing = true
	for mountPoint := range srv.connectionMap {
		err := fuse.Unmount(mountPoint)
		if err != nil {
			srv.mountLock.Unlock()
			return err
		}
	}
	for len(srv.connectionMap) != 0 {
		srv.mountCond.Wait()
	}
	srv.mountLock.Unlock()

	return srv.Storage.Close()
}

func (srv *Server) Mount(mountPoint string, contentAddress []byte, readOnly bool, options ...fuse.MountOption) error {
	rootInode, err := srv.Storage.LookupAddressInode(contentAddress)
	if err != nil {
		return err
	} else if rootInode == nil {
		return errors.New("could not find root content address")
	}

	var inodeMap *storage.InodeMap
	if rootInode.Mode == storage.MODE_HARDLINK_LAYER {
		newRootInode, err := srv.Storage.LookupAddressInode(rootInode.PathHash[:])
		if err != nil {
			return err
		} else if newRootInode == nil {
			return errors.New("reference data layer missing")
		}

		// Read hlmap from Address
		inodeMap = &storage.InodeMap{}
		hlf, err := srv.Storage.Cas.Open(rootInode.XattrAddress[:])
		if hlf == nil {
			return errors.New("could not read inode map")
		}
		err = inodeMap.Read(hlf)
		if err != nil {
			hlf.Close()
			return err
		}
		err = hlf.Close()
		if err != nil {
			return err
		}

		rootInode = newRootInode
	}

	mountPoint, err = filepath.Abs(mountPoint)
	if err != nil {
		return errors.New("failed to get absolute path of mount point")
	}

	srv.mountLock.Lock()
	defer srv.mountLock.Unlock()
	if srv.closing {
		return errors.New("server is already closed or closing")
	}
	_, found := srv.connectionMap[mountPoint]
	if found {
		return errors.New("mount already exists")
	}

	options = append(options, fuse.Subtype("ctrfs"))
	if readOnly {
		options = append(options, fuse.ReadOnly())
	}
	conn, err := fuse.Mount(mountPoint, options...)
	if err != nil {
		return err
	}

	ctrfsConn := &Connection{
		Conn:       conn,
		Storage:    srv.Storage,
		MountPoint: mountPoint,
		ReadOnly:   readOnly,
		rootInode:  *rootInode,
		inodeMap:   inodeMap,
		handleMap:  make(map[fuse.HandleID]Handle),
	}

	srv.connectionMap[mountPoint] = ctrfsConn
	srv.mountCond.Broadcast()

	go func() {
		err := ctrfsConn.Serve()
		if err == io.EOF {
			log.Printf("Connection unmounted at '%s'", mountPoint)
		} else {
			log.Printf("Connection '%s' shutting down do to '%s'", mountPoint, err)
		}

		srv.mountLock.Lock()
		delete(srv.connectionMap, mountPoint)
		srv.mountCond.Broadcast()
		defer srv.mountLock.Unlock()
	}()
	return nil
}
