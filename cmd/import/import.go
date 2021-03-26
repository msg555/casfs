package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/msg555/casfs/storage"
)

func main() {
	sc, err := storage.OpenDefaultStorageContext()
	if err != nil {
		log.Fatal(err)
	}

	for _, dir := range os.Args[1:] {
		nd, err := sc.ImportPath(dir)
		if err != nil {
			fmt.Println("Import of", dir, "failed with", err)
		} else {
			fmt.Println("Imported", dir, "successfully as", hex.EncodeToString(nd.NodeAddress), "at block", nd.NodeIndex)
		}
	}
}
