package blockfile

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/msg555/ctrfs/blockcache"
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
		Cache: blockcache.New(100, 32),
		File:  f,
	}
	bf.Init()
	defer func() {
		err := bf.Close()
		if err != nil {
			t.Fatalf("failed to close temp file '%s'", err)
		}
	}()

	ind, err := bf.Allocate(nil)
	if err != nil {
		t.Fatalf("failed to allocate block '%s'", err)
	}

	myData := []byte("01234567890123456789012345678901")
	err = bf.Write(nil, ind, myData)
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

	err = bf.WriteAt(nil, ind, 4, []byte("wow"))
	if err != nil {
		t.Fatalf("failed to writeat block '%s'", err)
	}

	dataIn, err = bf.Read(ind, nil)
	if err != nil {
		t.Fatalf("failed to readat block '%s'", err)
	}
	if !bytes.Equal([]byte("0123wow7890123456789012345678901"), dataIn) {
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
		Cache: blockcache.New(100, 32),
		File:  f,
	}
	bf.Init()
	defer func() {
		err := bf.Close()
		if err != nil {
			t.Fatalf("failed to close temp file '%s'", err)
		}
	}()

	ind, err := bf.Allocate(nil)
	if err != nil {
		t.Fatalf("failed to allocate block '%s'", err)
	}

	err = bf.Write(nil, ind, []byte("012345"))
	if err == nil {
		t.Fatalf("expected error due to write too small")
	}

	err = bf.Write(nil, ind, []byte("0123456789012345678901234567890123"))
	if err == nil {
		t.Fatalf("expected error due to write too large")
	}

	buf := make([]byte, 1, 40)
	nbuf, err := bf.Read(ind, buf)
	if err != nil {
		t.Fatalf("failed to read block '%s'", err)
	}
	if &nbuf[0] != &buf[0] {
		t.Fatalf("unexpected allocation of buffer")
	}
	if len(nbuf) != 32 {
		t.Fatalf("unexpected read result length")
	}

	_, err = bf.ReadAt(ind, -5, 3, buf)
	if err == nil {
		t.Fatalf("expected error due to negative offset")
	}
	_, err = bf.ReadAt(ind, 0, 33, buf)
	if err == nil {
		t.Fatalf("expected error due to too large size")
	}
	_, err = bf.ReadAt(ind, 4, 30, buf)
	if err == nil {
		t.Fatalf("expected error due to read too far")
	}
	_, err = bf.ReadAt(ind, 0, 32, buf)
	if err != nil {
		t.Fatalf("unexpected failure to read whole buffer '%s'", err)
	}

	err = bf.WriteAt(nil, ind, -5, buf[:3])
	if err == nil {
		t.Fatalf("expected error due to negative offset")
	}
	err = bf.WriteAt(nil, ind, 0, buf[:33])
	if err == nil {
		t.Fatalf("expected error due to too large size")
	}
	err = bf.WriteAt(nil, ind, 4, buf[:30])
	if err == nil {
		t.Fatalf("expected error due to read too far")
	}
	err = bf.WriteAt(nil, ind, 0, buf[:32])
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
		Cache: blockcache.New(100, 32),
		File:  f,
	}
	bf.Init()
	defer func() {
		err := bf.Close()
		if err != nil {
			t.Fatalf("failed to close temp file '%s'", err)
		}
	}()

	maxBlocks := 30
	blockIndex := make([]BlockIndex, maxBlocks)
	indexAllocated := make([]bool, 2*maxBlocks+1)
	rng := rand.New(rand.NewSource(555))

	for i := 0; i < 10000; i++ {
		j := rng.Int() % maxBlocks
		tag := rng.Int() % 4
		if blockIndex[j] == 0 {
			ind, err := bf.Allocate(tag)
			if err != nil {
				t.Fatalf("failed to allocate block '%s'", err)
			}
			if maxBlocks == 0 {
				t.Fatalf("allocate should not return 0 unless there is an error")
			}
			if int64(len(indexAllocated)) <= ind {
				println(i, ind)
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

func TestTags(t *testing.T) {
	f, err := tempFileCreate()
	if err != nil {
		t.Fatalf("unexpected error creating temp file '%s'", err)
	}

	bf := BlockFile{
		Cache: blockcache.New(200, 32),
		File:  f,
	}
	bf.Init()
	defer func() {
		err := bf.Close()
		if err != nil {
			t.Fatalf("failed to close temp file '%s'", err)
		}
	}()

	rng := rand.New(rand.NewSource(555))

	numTags := 5
	blockIndex := make([]BlockIndex, 100)
	blockTag := make([]interface{}, 100)
	for i := 0; i < len(blockIndex); i++ {
		blockTag[i] = rng.Int() % numTags
		index, err := bf.Allocate(blockTag[i])
		if err != nil {
			t.Fatalf("failed to allocate '%s'", err)
		}
		blockIndex[i] = index
	}

	buf := make([]byte, bf.Cache.BlockSize)
	for i := 0; i < 10000; i++ {
		j := rng.Int() % len(blockIndex)
		index := blockIndex[j]
		blockTag[j] = rng.Int() % numTags

		err := bf.Write(blockTag[j], index, buf)
		if err != nil {
			t.Fatalf("unexpected error writing to block")
		}

		if i % 50 == 0 {
			flushTag := interface{}(rng.Int() % numTags)

			for k := 0; k < len(blockIndex); k++ {
				bf.Cache.Access(&bf, blockIndex[k], false, func(tag interface{}, _ []byte, found bool) (interface{}, bool, error) {
					if !found {
						t.Fatalf("unexpectedly couldn't find block in cache")
					}
					if tag != blockTag[k] {
						t.Fatalf("block had unexpected tag, expected=%d, got=%d", blockTag[k], tag)
					}
					if tag == flushTag {
						blockTag[k] = nil
					}
					return tag, false, nil
				})
			}

			err := bf.SyncTag(flushTag)
			if err != nil {
				t.Fatalf("error flushing tag")
			}

			for k := 0; k < len(blockIndex); k++ {
				bf.Cache.Access(&bf, blockIndex[k], false, func(tag interface{}, _ []byte, found bool) (interface{}, bool, error) {
					if !found {
						t.Fatalf("unexpectedly couldn't find block in cache")
					}
					if tag != blockTag[k] {
						t.Fatalf("block had unexpected tag, expected=%d, got=%d", blockTag[k], tag)
					}
					return tag, false, nil
				})
			}
		}
	}
}

func TestOverlay(t *testing.T) {
	f, err := tempFileCreate()
	if err != nil {
		t.Fatalf("unexpected error creating temp file '%s'", err)
	}

	bfLayer1 := BlockFile{
		Cache: blockcache.New(200, 32),
		File:  f,
	}
	bfLayer1.Init()
	defer func() {
		err := bfLayer1.Close()
		if err != nil {
			t.Fatalf("failed to close temp file '%s'", err)
		}
	}()

	var roIndexes []BlockIndex
	for i := 0; i < 10; i++ {
		index, err := bfLayer1.Allocate(nil)
		if err != nil {
			t.Fatalf("failed to allocate block '%s'", err)
		}
		roIndexes = append(roIndexes, index)
	}

	f, err = tempFileCreate()
	if err != nil {
		t.Fatalf("unexpected error creating temp file '%s'", err)
	}

	bfLayer2 := BlockFile{
		Cache: blockcache.New(200, 32),
		File:  f,
	}
	bfLayer2.Init()
	defer func() {
		err := bfLayer2.Close()
		if err != nil {
			t.Fatalf("failed to close temp file '%s'", err)
		}
	}()

	bf := BlockOverlayAllocator{}
	err = bf.Init(&bfLayer1, &bfLayer2)
	if err != nil {
		t.Fatalf("failed to init overlay allocator '%s'", err)
	}

	var wrIndexes []BlockIndex
	for i := 0; i < 10; i++ {
		index, err := bf.Allocate(nil)
		if err != nil {
			t.Fatalf("failed to allocate block '%s'", err)
		}
		println(index)
		wrIndexes = append(wrIndexes, index)
	}
}
