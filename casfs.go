package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"github.com/spf13/pflag"
)

const DURATION_DEFAULT time.Duration = time.Duration(1000000000 * 60 * 60)


type CasFS struct {
	Conn					*fuse.Conn
	Fail					chan error
	MountDir			string
	OverlayDir		string
	OverlayStatfs	syscall.Statfs_t

	NodeMapLock		sync.RWMutex
	NodeMap				map[fuse.NodeID]*NodeData
	NodeByPathMap map[string]*NodeData
	NextNodeId		fuse.NodeID

	HandleMapLock sync.RWMutex
	HandleMap			map[fuse.HandleID]HandleData
	NextHandleId	fuse.HandleID
}

func CreateCasFS(mountDir, overlayDir string) (*CasFS, error) {
	cfs := &CasFS{
		Conn: nil,
		Fail: make(chan error, 1),
		MountDir: mountDir,
		OverlayDir: overlayDir,
		NodeMap: make(map[fuse.NodeID]*NodeData),
		NodeByPathMap: make(map[string]*NodeData),
		NextNodeId: FUSE_ROOT_ID + 1,
		HandleMap: make(map[fuse.HandleID]HandleData),
		NextHandleId: 1,
	}

	err := syscall.Statfs(cfs.OverlayDir, &cfs.OverlayStatfs)
	if err != nil {
		return nil, err
	}

	rootStat, err := doLstat(cfs.OverlayDir)
	if err != nil {
		return nil, err
	}
	if !S_ISDIR(rootStat.Mode) {
		return nil, err
	}
	cfs.NodeMap[FUSE_ROOT_ID] = &NodeData{
		Cfs: cfs,
		Node: FUSE_ROOT_ID,
		Path: ".",
		Stat: rootStat,
	}

	return cfs, nil
}

func (cfs *CasFS) AddNode(node *NodeData) fuse.NodeID {
	cfs.NodeMapLock.Lock()
	nodeId := cfs.NextNodeId
	cfs.NodeMap[nodeId] = node
	cfs.NodeByPathMap[node.Path] = node
	cfs.NextNodeId += 1
	cfs.NodeMapLock.Unlock()
	return nodeId
}

func (cfs *CasFS) AddHandle(handle HandleData) fuse.HandleID {
	cfs.HandleMapLock.Lock()
	handleId := cfs.NextHandleId
	cfs.HandleMap[handleId] = handle
	cfs.NextHandleId += 1
	cfs.HandleMapLock.Unlock()
	return handleId
}

func doLstat(path string) (syscall.Stat_t, error) {
	var st syscall.Stat_t;
	err := syscall.Lstat(path, &st)
	return st, err
}

const FUSE_ROOT_ID fuse.NodeID = 1

func (nd *NodeData) handleLookupRequest(req *fuse.LookupRequest) {
	cfs := nd.Cfs
	lookupPath := path.Join(nd.Path, req.Name)

	cfs.NodeMapLock.RLock()
	newNode, ok := cfs.NodeByPathMap[lookupPath]
	cfs.NodeMapLock.RUnlock()

	if ok {
		req.Respond(&fuse.LookupResponse{
			Node: newNode.Node,
			Generation: 1,
			EntryValid: DURATION_DEFAULT,
			Attr: newNode.GetAttr(),
		})
		return
	}

	lookupStat, err := doLstat(path.Join(cfs.OverlayDir, lookupPath))
	if err != nil {
		req.RespondError(WrapIOError(err))
		return
	}

	newNode = &NodeData{
		Cfs: cfs,
		Path: lookupPath,
		Stat: lookupStat,
	}

	// TODO: There's a race condition where the node may have already been
	// created.
	newNode.Node = nd.Cfs.AddNode(newNode)
	req.Respond(&fuse.LookupResponse{
		Node: newNode.Node,
		Generation: 1,
		EntryValid: DURATION_DEFAULT,
		Attr: newNode.GetAttr(),
	})
	fmt.Println("RESPONDING OKAY:", newNode.GetAttr())
}

func (nd *NodeData) handleAccessRequest(req *fuse.AccessRequest) {
	if nd.TestAccess(req, req.Mask) {
		req.Respond()
	} else {
		req.RespondError(FuseError{
			source: errors.New("permission denied"),
			errno: syscall.EACCES,
		})
	}
}

func (nd *NodeData) handleGetattrRequest(req *fuse.GetattrRequest) {
	fmt.Println("GETATTR:", req.Handle, req.Flags)
	req.Respond(&fuse.GetattrResponse{
		Attr: nd.GetAttr(),
	})
	fmt.Println("Req OKAY:", nd.GetAttr())
}


func (nd *NodeData) handleOpenRequest(req *fuse.OpenRequest) {
/*
	isRead := req.Flags.IsReadOnly() || req.Flags.IsReadWrite()
	isWrite := req.Flags.IsWriteOnly() || req.Flags.IsReadWrite()
*/
	if req.Dir {
		if !S_ISDIR(nd.Stat.Mode) {
			req.RespondError(FuseError{
				source: errors.New("not a directory"),
				errno: syscall.ENOTDIR,
			})
			return
		}

		if !nd.TestAccess(req, 4) {
			req.RespondError(FuseError{
				source: errors.New("permission denied"),
				errno: syscall.EACCES,
			})
			return
		}

		fd, err := syscall.Open(path.Join(nd.Cfs.OverlayDir, nd.Path), syscall.O_DIRECTORY, 0)
		if err != nil {
			req.RespondError(WrapIOError(err))
			return
		}

		fmt.Println(fd, err)
		handle := &DirectoryHandle{fd}
		handleId := nd.Cfs.AddHandle(handle)

		fmt.Println("Made handle", handleId)
		req.Respond(&fuse.OpenResponse{
			Handle: handleId,
			Flags: 0,
		})
	} else {
		req.RespondError(errors.New("not implemented"))
	}
}

func (nd *NodeData) handleReleaseRequest(req *fuse.ReleaseRequest) {
	nd.Cfs.HandleMapLock.RLock()
	hd, ok := nd.Cfs.HandleMap[req.Handle]
	nd.Cfs.HandleMapLock.RUnlock()
	if !ok {
		req.RespondError(FuseError{
			source: errors.New("permission denied"),
			errno: syscall.EBADF,
		})
		return
	}
	hd.Release(req)

	nd.Cfs.HandleMapLock.Lock()
	delete(nd.Cfs.HandleMap, req.Handle)
	nd.Cfs.HandleMapLock.Unlock()
}

func (nd *NodeData) handleReadRequest(req *fuse.ReadRequest) {
	nd.Cfs.HandleMapLock.RLock()
	hd, ok := nd.Cfs.HandleMap[req.Handle]
	nd.Cfs.HandleMapLock.RUnlock()
	if !ok {
		req.RespondError(FuseError{
			source: errors.New("permission denied"),
			errno: syscall.EBADF,
		})
		return
	}

	hd.Read(req)
}

func (cfs *CasFS) handleStatfsRequest(req *fuse.StatfsRequest) {
	req.Respond(&fuse.StatfsResponse{
		Blocks:		cfs.OverlayStatfs.Blocks,
		Bfree:		cfs.OverlayStatfs.Bfree,
		Bavail:		cfs.OverlayStatfs.Bavail,
		Files:		cfs.OverlayStatfs.Files,
		Ffree:		cfs.OverlayStatfs.Ffree,
		Bsize:		uint32(cfs.OverlayStatfs.Bsize),
		Namelen:	uint32(cfs.OverlayStatfs.Namelen),
		Frsize:		uint32(cfs.OverlayStatfs.Frsize),
	})
}

func (cfs *CasFS) handleRequest(req fuse.Request) {
	fmt.Println("request:", req)

	cfs.NodeMapLock.RLock()
	nd, ok := cfs.NodeMap[req.Hdr().Node]
	cfs.NodeMapLock.RUnlock()
	if !ok {
		req.RespondError(errors.New("invalid node"))
		return
	}

	switch req.(type) {
		case *fuse.StatfsRequest:
			cfs.handleStatfsRequest(req.(*fuse.StatfsRequest))
		case *fuse.LookupRequest:
			nd.handleLookupRequest(req.(*fuse.LookupRequest))
		case *fuse.AccessRequest:
			nd.handleAccessRequest(req.(*fuse.AccessRequest))
		case *fuse.GetattrRequest:
			nd.handleGetattrRequest(req.(*fuse.GetattrRequest))
		case *fuse.OpenRequest:
			nd.handleOpenRequest(req.(*fuse.OpenRequest))

		// Handle methods
		case *fuse.ReleaseRequest:
			nd.handleReleaseRequest(req.(*fuse.ReleaseRequest))
		case *fuse.ReadRequest:
			nd.handleReadRequest(req.(*fuse.ReadRequest))

		default:
			fmt.Println("WARNING NOT IMPLEMENTED:", req)
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
/*
	err := syscall.Access(cfs.MountDir, 07)
	fmt.Println("Test Access:", err)

	rootStat, err := doLstat(cfs.MountDir)
	fmt.Println("Test Stat:", rootStat, err)

	fd, err := syscall.Open(cfs.MountDir, syscall.O_DIRECTORY, 0)
	fmt.Println("Mount open:", fd, err)

	var data [300]byte
	n, err := syscall.ReadDirent(fd, data[:])
	fmt.Println(n, data, err)

	syscall.Close(fd)
*/

	fd, err := syscall.Open(cfs.OverlayDir, syscall.O_DIRECTORY, 0)

	var data[200]byte
	n, err := syscall.ReadDirent(fd, data[:])
	fmt.Println(n, err, data)

	syscall.Close(fd)
}

func main() {
	pflag.Parse()

	if pflag.NArg() != 2 {
		fmt.Println("Must specify mount point and mirror directory")
		os.Exit(1)
	}

	cfs, err := CreateCasFS(pflag.Arg(0), pflag.Arg(1))
	if err != nil {
		fmt.Println("Failed to initialize:", err)
		os.Exit(1)
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
	if err != nil {
		fmt.Println("Could not unmount:", err)
		os.Exit(1)
	}

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
