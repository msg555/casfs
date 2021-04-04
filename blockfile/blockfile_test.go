package blockfile

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func tempFileCreate() (*os.File, error) {
	f, err := ioutil.TempFile("", "ctrfs-test")
	if err != nil {
		return nil, err
	}
	err = os.Remove(f.Name())
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func TestWriteRead(t *testing.T) {
	f, err := tempFileCreate()
	if err != nil {
		t.Fatalf("unexpected error creating temp file '%s'", err)
	}

	bf := BlockFile{
		BlockSize: 16,
	}
	err = bf.OpenFile(f)
	if err != nil {
		t.Fatalf("failed to open block file '%s'", err)
	}
	defer func() {
		err := bf.Close()
		if err != nil {
			t.Fatalf("failed to close temp file '%s'", err)
		}
	}()

	ind, err := bf.Allocate()
	if err != nil {
		t.Fatalf("failed to allocate block '%s'", err)
	}

	myData := []byte("0123456789012345")
	err = bf.Write(ind, myData)
	if err != nil {
		t.Fatalf("failed to write block '%s'", err)
	}

	dataIn, err := bf.Read(ind, nil)
	if err != nil {
		t.Fatalf("failed to readat block '%s'", err)
	}
	if !bytes.Equal(myData, dataIn) {
		t.Fatalf("got unexpected data back from read '%s'", err)
	}

	err = bf.WriteAt(ind, 4, []byte("wow"))
	if err != nil {
		t.Fatalf("failed to writeat block '%s'", err)
	}

	dataIn, err = bf.Read(ind, nil)
	if err != nil {
		t.Fatalf("failed to readat block '%s'", err)
	}
	if !bytes.Equal([]byte("0123wow789012345"), dataIn) {
		t.Fatalf("got unexpected data back from read '%s'", err)
	}

	dataIn, err = bf.ReadAt(ind, 4, 3, nil)
	if err != nil {
		t.Fatalf("failed to readat block '%s'", err)
	}
	if !bytes.Equal([]byte("wow"), dataIn) {
		t.Fatalf("got unexpected data back from readat '%s'", err)
	}
}

func TestBounds(t *testing.T) {
	f, err := tempFileCreate()
	if err != nil {
		t.Fatalf("unexpected error creating temp file '%s'", err)
	}

	bf := BlockFile{
		BlockSize: 16,
	}
	err = bf.OpenFile(f)
	if err != nil {
		t.Fatalf("failed to open block file '%s'", err)
	}
	defer func() {
		err := bf.Close()
		if err != nil {
			t.Fatalf("failed to close temp file '%s'", err)
		}
	}()

	ind, err := bf.Allocate()
	if err != nil {
		t.Fatalf("failed to allocate block '%s'", err)
	}

	err = bf.Write(ind, []byte("012345"))
	if err == nil {
		t.Fatalf("expected error do to write too small")
	}

	err = bf.Write(ind, []byte("01234567890123456"))
	if err == nil {
		t.Fatalf("expected error do to write too large")
	}

	buf := make([]byte, 1, 20)
	nbuf, err := bf.Read(ind, buf)
	if err != nil {
		t.Fatalf("failed to read block '%s'", err)
	}
	if &nbuf[0] != &buf[0] {
		t.Fatalf("unexpected allocation of buffer")
	}
	if len(nbuf) != 16 {
		t.Fatalf("unexpected read result length")
	}

	_, err = bf.ReadAt(ind, -5, 3, buf)
	if err == nil {
		t.Fatalf("expected error do to negative offset")
	}
	_, err = bf.ReadAt(ind, 0, 17, buf)
	if err == nil {
		t.Fatalf("expected error do to too large size")
	}
	_, err = bf.ReadAt(ind, 4, 14, buf)
	if err == nil {
		t.Fatalf("expected error do to read too far")
	}
	_, err = bf.ReadAt(ind, 0, 16, buf)
	if err != nil {
		t.Fatalf("unexpected failure to read whole buffer '%s'", err)
	}

	err = bf.WriteAt(ind, -5, buf[:3])
	if err == nil {
		t.Fatalf("expected error do to negative offset")
	}
	err = bf.WriteAt(ind, 0, buf[:17])
	if err == nil {
		t.Fatalf("expected error do to too large size")
	}
	err = bf.WriteAt(ind, 4, buf[:14])
	if err == nil {
		t.Fatalf("expected error do to read too far")
	}
	err = bf.WriteAt(ind, 0, buf[:16])
	if err != nil {
		t.Fatalf("unexpected failure to write whole buffer '%s'", err)
	}
}

func TestAllocateFree(t *testing.T) {
	f, err := tempFileCreate()
	if err != nil {
		t.Fatalf("unexpected error creating temp file '%s'", err)
	}

	bf := BlockFile{
		BlockSize: 16,
	}
	err = bf.OpenFile(f)
	if err != nil {
		t.Fatalf("failed to open block file '%s'", err)
	}
	defer func() {
		err := bf.Close()
		if err != nil {
			t.Fatalf("failed to close temp file '%s'", err)
		}
	}()

	maxBlocks := 8
	blockIndex := make([]BlockIndex, maxBlocks)
	indexAllocated := make([]bool, maxBlocks+1)
	rng := rand.New(rand.NewSource(555))

	for i := 0; i < 1024; i++ {
		j := rng.Int() % maxBlocks
		if blockIndex[j] == 0 {
			ind, err := bf.Allocate()
			if err != nil {
				t.Fatalf("failed to allocate block '%s'", err)
			}
			if maxBlocks == 0 {
				t.Fatalf("allocate should not return 0 unless there is an error")
			}
			if BlockIndex(maxBlocks) < ind {
				t.Fatalf("allocated index is too large")
			}
			if indexAllocated[ind] {
				t.Fatalf("block already allocated")
			}
			blockIndex[j] = ind
			indexAllocated[ind] = true
		} else {
			ind := blockIndex[j]
			err := bf.Free(ind)
			if err != nil {
				t.Fatalf("failed to free block '%s'", err)
			}
			blockIndex[j] = 0
			indexAllocated[ind] = false
		}
	}
}
