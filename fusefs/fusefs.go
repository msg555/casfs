package fusefs

import (
	"bazil.org/fuse"
	"errors"
	"fmt"
)

type FuseCasfsConnection struct {
	Conn       *fuse.Conn
	Server     *FuseCasfsServer
	MountPoint string
	ReadOnly   bool
}

func (conn *FuseCasfsConnection) Serve() {
	for {
		req, err := conn.Conn.ReadRequest()
		if err != nil {
			conn.Server.Fail <- err
			return
		}
		go conn.handleRequest(req)
	}
}

func (conn *FuseCasfsConnection) handleRequest(req fuse.Request) {
	switch req.(type) {
	default:
		fmt.Println("WARNING NOT IMPLEMENTED:", req)
		req.RespondError(errors.New("not implemented"))
	}
}

func (conn *FuseCasfsConnection) Close() error {
	err := fuse.Unmount(conn.MountPoint)
	if err != nil {
		return err
	}
	return conn.Conn.Close()
}
