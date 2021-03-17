package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/msg555/casfs/fusefs"

	"bazil.org/fuse"
  "github.com/spf13/pflag"
)

func testIt(cfs *fusefs.CasFS) {
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

	var data [200]byte
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

  syscall.Umask(0)

  cfs, err := fusefs.CreateCasFS(pflag.Arg(0), pflag.Arg(1))
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

  go cfs.Serve()
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
