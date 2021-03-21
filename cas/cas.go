// TODO: GC

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
)

type HashFactory func()hash.Hash

type Castore struct {
	StorageRoot	string
	WorkDir			string
	HashFactory
}

type ContentAddress []byte


const TEMP_DIR string = "temp"


func CreateCastore(storageRoot string, hashFactory HashFactory) (*Castore, error) {
	storageRoot, err := filepath.Abs(storageRoot)
	if err != nil {
		return nil, err
	}

	cas := Castore{
		StorageRoot: storageRoot,
		WorkDir: path.Join(storageRoot, TEMP_DIR),
	}
	if hashFactory != nil {
		cas.HashFactory = hashFactory
	} else {
		cas.HashFactory = sha256.New
	}

	err = os.MkdirAll(cas.WorkDir, 0777)
	if err != nil {
		return nil, err
	}
	return &cas, nil
}

func (cas *Castore) objectPath(addr ContentAddress) (string, string) {
	hexAddr := hex.EncodeToString(addr)
	fmt.Println(hexAddr)

	return path.Join(
		cas.StorageRoot,
		hexAddr[0:2],
		hexAddr[2:4],
	), hexAddr[4:]
}

func (cas *Castore) Open(addr ContentAddress) (*os.File, error) {
	objDir, objName := cas.objectPath(addr)
	return os.Open(path.Join(objDir, objName))
}

func (cas *Castore) Insert(data io.Reader) (ContentAddress, error) {
	f, err := os.CreateTemp(cas.WorkDir, "data-")
	if err != nil {
		return nil, err
	}

	hasher := cas.HashFactory()
	_, err = io.Copy(io.MultiWriter(hasher, f), data)
	if err != nil {
		return nil, err
	}

	err = f.Close()
	if err != nil {
		return nil, err
	}

	h := hasher.Sum(nil)

	objDir, objName := cas.objectPath(h)
	err = os.MkdirAll(objDir, 0777)
	if err != nil {
		return nil, err
	}

	err = os.Rename(f.Name(), path.Join(objDir, objName))
	if err != nil {
		return nil, err
	}

	return h, nil
}

func main() {
	fmt.Println("Hi")

	cas, err := CreateCastore("test", nil)
	if err != nil {
		panic(err)
	}

	r := bytes.NewReader([]byte("Hello world!\njk\n"))
	addr, err := cas.Insert(r)
	if err != nil {
		panic(err)
	}

	fmt.Println("Got address:", addr)

	fread, err := cas.Open(addr)
	if err != nil {
		panic(err)
	}
	io.Copy(os.Stdout, fread)
}
