package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

import "bazil.org/fuse"
import "github.com/spf13/pflag"


func handleGetattrRequest(req *fuse.GetattrRequest, fail chan error) {
	fmt.Println("Handling that attribute request: ", req.Header.Pid)
	req.RespondError(errors.New("not implemented"))
}


func handleRequest(req fuse.Request, fail chan error) {
	switch req.(type) {
		case *fuse.GetattrRequest:
			handleGetattrRequest(req.(*fuse.GetattrRequest), fail)
		default:
			fmt.Println("request: ", req)
			req.RespondError(errors.New("not implemented"))
	}
}

func serve(conn *fuse.Conn, fail chan error) {
	for {
		req, err := conn.ReadRequest()
		if err != nil {
			fail <- err
			return
		}
		go handleRequest(req, fail)
	}
}

func main() {
	pflag.Parse()

	if pflag.NArg() != 2 {
		fmt.Println("Must specify mount point and mirror directory")
		os.Exit(1)
	}

	mountPoint := pflag.Arg(0)
	// mirrorDir := pflag.Arg(1)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	conn, err := fuse.Mount(mountPoint)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fail := make(chan error, 1)
	go serve(conn, fail)

	select {
	case err := <-fail:
		fmt.Println(err)
	case sig := <-sigs:
		fmt.Println("signal received: ", sig)
	}
	err = fuse.Unmount(mountPoint)

	err = conn.Close()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
