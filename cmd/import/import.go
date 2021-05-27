package main

import (
	"fmt"
	"log"
	"os"

	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/storage"
)

func help() {
	fmt.Printf("%s (dir|tar) file [file ...]\n", os.Args[0])
}

func fail(err error, message string) {
	gerr, ok := err.(*errors.Error)
	if ok {
		log.Fatalf("%s: %s\n%s", message, err, gerr.ErrorStack())
	}
	log.Fatalf("%s: %s", message, err)
}

func main() {
	if len(os.Args) < 3 {
		help()
		os.Exit(1)
	}

	mode := os.Args[1]
	if mode != "dir" && mode != "tar" {
		help()
		os.Exit(1)
	}

	sc, err := storage.OpenDefaultStorageContext()
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range os.Args[2:] {
		if file == "-" {
			file = "/dev/stdin"
		}

		mnt, err := sc.CreateEmptyMount()
		if err != nil {
			fail(err, "Failed to create new mount")
		}

		if mode == "dir" {
			inodeId, err := mnt.ImportPath(file)
			if err != nil {
				fail(err, "failed to import path")
			}
			if err := mnt.SetRootNode(inodeId); err != nil {
				fail(err, "failed to set root node")
			}
		} else {
			log.Fatal("only dir import supported")
		}
	}

	err = sc.Close()
	if err != nil {
		log.Fatalf("failed shutting down storage: %s", err)
	}
}
