package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/go-errors/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sys/unix"

	"github.com/msg555/ctrfs/fusefs"
)

func testIt(conn *fusefs.FuseCasfsConnection) {
	// err := unix.Access(cfs.MountDir, 0777)
	/*
		err := unix.Access(cfs.MountDir, 07)
		fmt.Println("Test Access:", err)

		rootStat, err := doLstat(cfs.MountDir)
		fmt.Println("Test Stat:", rootStat, err)

		fd, err := unix.Open(cfs.MountDir, unix.O_DIRECTORY, 0)
		fmt.Println("Mount open:", fd, err)

		var data [300]byte
		n, err := unix.ReadDirent(fd, data[:])
		fmt.Println(n, data, err)

		unix.Close(fd)
	*/

	/*
		fd, err := unix.Open(conn.OverlayDir, unix.O_DIRECTORY, 0)

		var data [200]byte
		n, err := unix.ReadDirent(fd, data[:])
		fmt.Println(n, err, data)

		unix.Close(fd)
	*/
}

func main() {
	pflag.Parse()
	if pflag.NArg() != 2 {
		fmt.Println("Must specify mount point and root address")
		os.Exit(1)
	}

	srv, err := fusefs.CreateDefaultServer()
	if err != nil {
		log.Fatal("failed to initialize", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGINT, unix.SIGTERM)

	rootAddress, err := hex.DecodeString(pflag.Arg(1))
	if err != nil {
		log.Fatal("failed to decode content address", err)
	}

	conn, err := srv.Mount(pflag.Arg(0), rootAddress, true)
	if err != nil {
		gerr, ok := err.(*errors.Error)
		if ok {
			log.Fatal(err, gerr.ErrorStack())
		} else {
			log.Fatal(err)
		}
	}

	go conn.Serve()
	go testIt(conn)

	select {
	case err := <-srv.Fail:
		fmt.Println(err)
	case sig := <-sigs:
		fmt.Println("signal received: ", sig)
	}
	err = conn.Close()
	if err != nil {
		log.Fatal("Could not unmount:", err)
	}
}
