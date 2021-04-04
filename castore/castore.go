// TODO: GC

package castore

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

type HashFactory func() hash.Hash

type Castore struct {
	StorageRoot string
	WorkDir     string
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
		WorkDir:     path.Join(storageRoot, TEMP_DIR),
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

func (cas *Castore) Insert(data io.Reader) (ContentAddress, int64, error) {
	f, err := ioutil.TempFile(cas.WorkDir, "data-")
	if err != nil {
		return nil, 0, err
	}

	hasher := cas.HashFactory()
	written, err := io.Copy(io.MultiWriter(hasher, f), data)
	if err != nil {
		return nil, 0, err
	}

	err = f.Close()
	if err != nil {
		return nil, 0, err
	}

	h := hasher.Sum(nil)

	objDir, objName := cas.objectPath(h)
	err = os.MkdirAll(objDir, 0777)
	if err != nil {
		return nil, 0, err
	}

	err = os.Rename(f.Name(), path.Join(objDir, objName))
	if err != nil {
		return nil, 0, err
	}

	return h, written, nil
}
