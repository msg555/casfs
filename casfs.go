package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
)

import "bazil.org/fuse"
import "github.com/spf13/pflag"


func S_ISDIR(mode uint32) bool {
	return ((mode & 0170000) == 0040000)
}


type NodeData struct {
	Path				string
	Stat				syscall.Stat_t
	ParentNode	fuse.NodeID
}

func (nd NodeData) TestAccess(req fuse.Request, mask uint32) bool {
	hdr := req.Hdr()
	mode := uint32(nd.Stat.Mode)

	modeEffective := mode & 07
	if hdr.Uid == nd.Stat.Uid {
		modeEffective |= (mode >> 6) & 07
	}
	if hdr.Gid == nd.Stat.Gid {
		modeEffective |= (mode >> 6) & 07
	}
	return (mask & modeEffective) == mask
}

type CasFS struct {
	Conn				*fuse.Conn
	Fail				chan error
	MountDir		string
	OverlayDir	string

	NodeMapLock sync.RWMutex
	NodeMap			map[fuse.NodeID]NodeData
	NextNodeId  fuse.NodeID
}

type FuseError struct {
	source      error
	errno				syscall.Errno
}

func (err FuseError) Error() string {
	return err.source.Error()
}

func (err FuseError) Errno() fuse.Errno {
	return fuse.Errno(err.errno)
}

func doLstat(path string) (syscall.Stat_t, error) {
	var st syscall.Stat_t;
	err := syscall.Lstat(path, &st)
	return st, err
}

func WrapIOError(err error) FuseError {
	e := err
	for {
		switch e.(type) {
			case *os.PathError:
				e = e.(*os.PathError).Err
			default:
				return FuseError{
					source: err,
					errno: syscall.EIO,
				}
		}
	}
}


const FUSE_ROOT_ID fuse.NodeID = 1

func (cfs *CasFS) handleLookupRequest(req *fuse.LookupRequest) {
	nodeData := cfs.NodeMap[req.Node]
	if nodeData.Path == "" {
		req.RespondError(errors.New("invalid node"))
	}

	lookupPath := path.Join(nodeData.Path, req.Name)

	lookupStat, err := doLstat(path.Join(cfs.OverlayDir, lookupPath))
	if err != nil {
		req.RespondError(WrapIOError(err))
		return
	}

	cfs.NodeMapLock.Lock()
	cfs.NodeMap[cfs.NextNodeId] = NodeData{
		Path: lookupPath,
		Stat: lookupStat,
	}
	cfs.NextNodeId += 1
	cfs.NodeMapLock.Unlock()
}

func (cfs *CasFS) handleAccessRequest(req *fuse.AccessRequest) {
	nodeData := cfs.NodeMap[req.Node]
	if nodeData.Path == "" {
		req.RespondError(errors.New("invalid node"))
	}

	if nodeData.TestAccess(req, req.Mask) {
		req.Respond()
	} else {
		req.RespondError(FuseError{
			source: errors.New("permission denied"),
			errno: syscall.EACCES,
		})
	}
}

func (cfs *CasFS) handleGetattrRequest(req *fuse.GetattrRequest) {
	// fmt.Println("Handling that attribute request: ", req.Header.Pid)
	req.RespondError(errors.New("not implemented"))
}


func (cfs *CasFS) handleRequest(req fuse.Request) {
	switch req.(type) {
		case *fuse.LookupRequest:
			cfs.handleLookupRequest(req.(*fuse.LookupRequest))
		case *fuse.AccessRequest:
			cfs.handleAccessRequest(req.(*fuse.AccessRequest))
		case *fuse.GetattrRequest:
			cfs.handleGetattrRequest(req.(*fuse.GetattrRequest))
		default:
			// fmt.Println("request: ", req)
			req.RespondError(errors.New("not implemented"))
	}
}

func (cfs *CasFS) serve() {
	for {
		req, err := cfs.Conn.ReadRequest()
		if err != nil {
			cfs.Fail <- err
			return
		}
		go cfs.handleRequest(req)
	}
}

func testIt(cfs *CasFS) {
	// err := syscall.Access(cfs.MountDir, 0777)
	err := syscall.Access(cfs.MountDir, 07)

	fmt.Println("Test Access:", err)
}

func main() {
	pflag.Parse()

	if pflag.NArg() != 2 {
		fmt.Println("Must specify mount point and mirror directory")
		os.Exit(1)
	}

	cfs := &CasFS{
		Conn: nil,
		Fail: make(chan error, 1),
		MountDir: pflag.Arg(0),
		OverlayDir: pflag.Arg(1),
		NodeMap: make(map[fuse.NodeID]NodeData),
		NextNodeId: FUSE_ROOT_ID + 1,
	}

	rootStat, err := doLstat(cfs.OverlayDir)
	if err != nil {
		fmt.Println("Failed to access overlay dir:", err)
		os.Exit(1)
	}
	if !S_ISDIR(rootStat.Mode) {
		fmt.Println("Overlay muste be a directory")
		os.Exit(1)
	}
	cfs.NodeMap[FUSE_ROOT_ID] = NodeData{
		Path: ".",
		Stat: rootStat,
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	cfs.Conn, err = fuse.Mount(cfs.MountDir)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	go cfs.serve()
	go testIt(cfs)

	select {
	case err := <-cfs.Fail:
		fmt.Println(err)
	case sig := <-sigs:
		fmt.Println("signal received: ", sig)
	}
	err = fuse.Unmount(cfs.MountDir)

	err = cfs.Conn.Close()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
