package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/msg555/ctrfs/blockcache"
	"github.com/msg555/ctrfs/blockfile"
)

func blockFileCreate(cache *blockcache.BlockCache) (blockfile.BlockAllocator, error) {
	f, err := ioutil.TempFile("", "ctrfs-test")
	if err != nil {
		return nil, err
	}
	err = os.Remove(f.Name())
	if err != nil {
		f.Close()
		return nil, err
	}
	bf := blockfile.BlockFile{
		Cache: cache,
		File:  f,
	}
	bf.Init()
	return &bf, nil
}

func fillPath(t *testing.T, tf *TreeFile, length int) {
	buf := make([]byte, length)
	for i := 0; i < length; i++ {
		buf[i] = byte(i)
	}
	n, err := tf.WriteAt(buf, 0)
	if err != nil {
		t.Fatalf("failed writing data '%s'", err)
	}
	if n != length {
		t.Fatal("wrote unexpected number of bytes")
	}
}

func TestReadWrite(t *testing.T) {
	cache := blockcache.New(20, 4096)
	bf1, err := blockFileCreate(cache)
	if err != nil {
		t.Fatalf("unexpected error creating block file '%s'", err)
	}
	defer bf1.Close()

	tm1 := TreeFileManager{}
	tm1.Init(bf1, &NullInodeMap{})

	tf1, err := tm1.NewFile(&InodeData{})
	if err != nil {
		t.Fatalf("error creating new file '%s'", err)
	}
	fillPath(t, tf1, 10000)

	if tf1.GetInode().Size != 10000 {
		t.Fatal("unexpected file size after write")
	}

	bf2, err := blockFileCreate(cache)
	if err != nil {
		t.Fatalf("unexpected error creating block file '%s'", err)
	}
	defer bf2.Close()

	bfOverlay := &blockfile.BlockOverlayAllocator{}
	bfOverlay.Init(bf1, bf2)
	defer bfOverlay.Close()

	imap := InodeTreeMap{}
	err = imap.Init(bfOverlay)
	if err != nil {
		t.Fatalf("unexpected error creating inode map '%s'", err)
	}

	tm2 := TreeFileManager{}
	tm2.Init(bfOverlay, &imap)

	tf2, err := tm2.OpenFile(tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}

	off := 4040
	buf := make([]byte, 100)
	_, err = tf2.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(buf); i++ {
		if buf[i] != byte(off+i) {
			t.Fatal("got unexpected data from read")
		}
	}

	off = 7000
	msg := []byte("hello world")
	_, err = tf2.WriteAt(msg, int64(off))
	if err != nil {
		t.Fatal(err)
	}

	off -= 10
	_, err = tf2.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(buf); i++ {
		if 10 <= i && i < 10+len(msg) {
			if buf[i] != msg[i-10] {
				t.Fatal("unexpected read after write")
			}
		} else if buf[i] != byte(off+i) {
			t.Fatal("unexpected read after write")
		}
	}

	err = tf2.Close()
	if err != nil {
		t.Fatal(err)
	}

	tf2, err = tm2.OpenFile(tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}

	_, err = tf2.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(buf); i++ {
		if 10 <= i && i < 10+len(msg) {
			if buf[i] != msg[i-10] {
				t.Fatal("unexpected read after write")
			}
		} else if buf[i] != byte(off+i) {
			t.Fatal("unexpected read after write")
		}
	}
}

func TestTruncate(t *testing.T) {
	cache := blockcache.New(20, 4096)
	bf1, err := blockFileCreate(cache)
	if err != nil {
		t.Fatalf("unexpected error creating block file '%s'", err)
	}
	defer bf1.Close()

	tm1 := TreeFileManager{}
	tm1.Init(bf1, &NullInodeMap{})

	tf1, err := tm1.NewFile(&InodeData{})
	if err != nil {
		t.Fatalf("error creating new file '%s'", err)
	}
	fillPath(t, tf1, 10000)

	if tf1.GetInode().Size != 10000 {
		t.Fatal("unexpected file size after write")
	}

	bf2, err := blockFileCreate(cache)
	if err != nil {
		t.Fatalf("unexpected error creating block file '%s'", err)
	}
	defer bf2.Close()

	bfOverlay := &blockfile.BlockOverlayAllocator{}
	bfOverlay.Init(bf1, bf2)
	defer bfOverlay.Close()

	imap := InodeTreeMap{}
	err = imap.Init(bfOverlay)
	if err != nil {
		t.Fatalf("unexpected error creating inode map '%s'", err)
	}

	tm2 := TreeFileManager{}
	tm2.Init(bfOverlay, &imap)

	tf2, err := tm2.OpenFile(tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}

	off := 9950
	buf := make([]byte, 100)
	n, err := tf2.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}
	if n != 50 {
		t.Fatalf("read unexpected number of bytes, wanted=50 got=%d", n)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte(off+i) {
			t.Fatal("unexpected data read")
		}
	}

	tf2.UpdateInode(func(inodeData *InodeData) error {
		inodeData.Size = 5000
		return nil
	})
	n, err = tf2.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("read unexpected number of bytes, wanted=0 got=%d", n)
	}

	off = 4950
	n, err = tf2.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}
	if n != 50 {
		t.Fatalf("read unexpected number of bytes, wanted=50 got=%d", n)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte(off+i) {
			t.Fatal("unexpected data read")
		}
	}

	extraData := []byte("hellolookatme")
	_, err = tf2.WriteAt(extraData, 10000)
	if err != nil {
		t.Fatal(err)
	}

	tf2.UpdateInode(func(inodeData *InodeData) error {
		if inodeData.Size != uint64(10000+len(extraData)) {
			t.Fatal("file size did not increase with write at")
		}
		inodeData.Size = 10000
		return nil
	})
	off = 9950
	n, err = tf2.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}
	if n != 50 {
		t.Fatalf("read unexpected number of bytes, wanted=50 got=%d", n)
	}
	for i := 0; i < n; i++ {
		if buf[i] != 0 {
			t.Fatal("unexpected data read")
		}
	}

	err = tf2.Close()
	if err != nil {
		t.Fatal(err)
	}

	tf2, err = tm2.OpenFile(tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}

	n, err = tf2.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}
	if n != 50 {
		t.Fatalf("read unexpected number of bytes, wanted=50 got=%d", n)
	}
	for i := 0; i < n; i++ {
		if buf[i] != 0 {
			t.Fatal("unexpected data read")
		}
	}

	tf2.UpdateInode(func(inodeData *InodeData) error {
		inodeData.Size = 20000
		return nil
	})

	n, err = tf2.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("read unexpected number of bytes, wanted=50 got=%d", n)
	}
	for i := 0; i < n; i++ {
		if buf[i] != 0 {
			println(i)
			fmt.Println(buf)
			t.Fatal("unexpected data read")
		}
	}
}
