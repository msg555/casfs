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

	err = srv.Mount(pflag.Arg(0), rootAddress, false)
	if err != nil {
		gerr, ok := err.(*errors.Error)
		if ok {
			log.Fatal(err, gerr.ErrorStack())
		} else {
			log.Fatal(err)
		}
	}

	fmt.Println("signal received: ", <-sigs)

	err = srv.Close()
	if err != nil {
		log.Fatal("Could not unmount:", err)
	}
}
