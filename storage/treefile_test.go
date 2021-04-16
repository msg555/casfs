package storage

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/msg555/ctrfs/blockcache"
	"github.com/msg555/ctrfs/blockfile"
	"github.com/msg555/ctrfs/unix"
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

func fillPath(t *testing.T, tf FileObjectReg, length int) {
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

func TestRegReadWrite(t *testing.T) {
	cache := blockcache.New(20, 4096)
	bf1, err := blockFileCreate(cache)
	if err != nil {
		t.Fatalf("unexpected error creating block file '%s'", err)
	}
	defer bf1.Close()

	tm1 := TreeFileManager{}
	tm1.Init(bf1, &NullInodeMap{})

	tfi1, err := tm1.NewFile(&InodeData{
		Mode: unix.S_IFREG,
	})
	if err != nil {
		t.Fatalf("error creating new file '%s'", err)
	}
	tf1 := tfi1.(FileObjectReg)
	fillPath(t, tf1, 10000)

	if tf1.GetInode().Size != 10000 {
		t.Fatal("unexpected file size after write")
	}
	if err := tf1.Close(); err != nil {
		t.Fatal("error closing layer 1 file")
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

	tfi2, err := tm2.OpenFile(unix.DT_REG, tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}
	tf2 := tfi2.(FileObjectReg)

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

	tfi2, err = tm2.OpenFile(unix.DT_REG, tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}
	tf2 = tfi2.(FileObjectReg)

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

func TestRegTruncate(t *testing.T) {
	cache := blockcache.New(20, 4096)
	bf1, err := blockFileCreate(cache)
	if err != nil {
		t.Fatalf("unexpected error creating block file '%s'", err)
	}
	defer bf1.Close()

	tm1 := TreeFileManager{}
	tm1.Init(bf1, &NullInodeMap{})

	tfi1, err := tm1.NewFile(&InodeData{
		Mode: unix.S_IFREG,
	})
	if err != nil {
		t.Fatalf("error creating new file '%s'", err)
	}
	tf1 := tfi1.(FileObjectReg)
	fillPath(t, tf1, 10000)

	if tf1.GetInode().Size != 10000 {
		t.Fatal("unexpected file size after write")
	}
	if err := tf1.Close(); err != nil {
		t.Fatal("error closing layer 1 file")
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

	tfi2, err := tm2.OpenFile(unix.DT_REG, tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}
	tf2 := tfi2.(FileObjectReg)

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

	tfi2, err = tm2.OpenFile(unix.DT_REG, tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}
	tf2 = tfi2.(FileObjectReg)

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
			t.Fatal("unexpected data read")
		}
	}
}

type direntHolder struct {
	DtType int
	InodeId
}

func fuzzDir(t *testing.T, rng *rand.Rand, tf FileObjectDir, state map[string]direntHolder) {
	keyDomain := 100
	for i := 0; i < 10000; i++ {
		name := fmt.Sprintf("%d", rng.Int()%keyDomain)
		stateEntry, ok := state[name]

		switch rng.Int() % 3 {
		case 0: // Lookup
			dtType, inodeId, err := tf.Lookup(name)
			if err != nil {
				t.Fatalf("lookup failed '%s'", err)
			}
			if inodeId == 0 {
				if ok {
					t.Fatalf("failed to find existing entry")
				}
			} else {
				if !ok {
					t.Fatalf("found non-existant entry")
				}
				if stateEntry.DtType != dtType {
					t.Fatalf("incorrect dt type stored")
				}
				if stateEntry.InodeId != inodeId {
					t.Fatalf("incorrect inode id stored")
				}
			}
		case 1: // Link
			overwrite := rng.Int()%2 == 0
			dtType := 1 + rng.Int()%16
			inodeId := rng.Int63()
			if err := tf.Link(name, dtType, inodeId, overwrite); err != nil {
				t.Fatalf("link failed '%s'", err)
			}
			if !ok || overwrite {
				state[name] = direntHolder{
					DtType:  dtType,
					InodeId: inodeId,
				}
			}
		case 2: // Unlink
			removed, err := tf.Unlink(name)
			if err != nil {
				t.Fatalf("unlink failed '%s'", err)
			}
			if ok && !removed {
				t.Fatal("did not remove existing element")
			}
			if !ok && removed {
				t.Fatal("removed non-existant element")
			}
			delete(state, name)
		}
		if i%100 == 0 { // Scan
			count := 0
			done, err := tf.Scan("", func(name string, dtType int, inodeId InodeId) bool {
				count++
				entry, ok := state[name]
				if !ok {
					t.Fatal("scan found element that does not exist")
				}
				if entry.DtType != dtType {
					t.Fatal("scan gave incorrect dt type")
				}
				if entry.InodeId != inodeId {
					t.Fatal("scan gave incorrect inode id")
				}
				return true
			})
			if err != nil {
				t.Fatalf("scan failed '%s'", err)
			}
			if !done {
				t.Fatal("scan unexpectedly did not finish")
			}
			if count != len(state) {
				t.Fatal("scan did not find all elements")
			}
		}
	}
}

func TestDirFuzz(t *testing.T) {
	cache := blockcache.New(20, 4096)
	bf1, err := blockFileCreate(cache)
	if err != nil {
		t.Fatalf("unexpected error creating block file '%s'", err)
	}
	defer bf1.Close()

	tm1 := TreeFileManager{}
	tm1.Init(bf1, &NullInodeMap{})

	tfi1, err := tm1.NewFile(&InodeData{
		Mode: unix.S_IFDIR,
	})
	if err != nil {
		t.Fatalf("error creating new dir '%s'", err)
	}
	tf1 := tfi1.(FileObjectDir)

	rng := rand.New(rand.NewSource(555))
	state := make(map[string]direntHolder)
	fuzzDir(t, rng, tf1, state)

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

	tfi2, err := tm2.OpenFile(unix.DT_DIR, tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}
	tf2 := tfi2.(FileObjectDir)

	fuzzDir(t, rng, tf2, state)

	err = tf2.Close()
	if err != nil {
		t.Fatal(err)
	}

	tfi2, err = tm2.OpenFile(unix.DT_DIR, tf1.GetInodeId())
	if err != nil {
		t.Fatal(err)
	}
	tf2 = tfi2.(FileObjectDir)

	fuzzDir(t, rng, tf2, state)
}
