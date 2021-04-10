package storage

import (
	"bufio"
	"io/ioutil"
	"os"
	"testing"

	"github.com/msg555/ctrfs/blockcache"
)

func createTempFile() (string, error) {
	f, err := ioutil.TempFile("", "ctrfs-test")
	if err != nil {
		return "", err
	}
	err = f.Close()
	if err != nil {
		return "", err
	}
	return f.Name(), nil
}

func fillPath(path string, length int) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	x := byte(0)
	w := bufio.NewWriter(f)
	defer w.Flush()

	for i := 0; i < length; i++ {
		err = w.WriteByte(x)
		if err != nil {
			return err
		}
		x++
	}

	return nil
}

func TestReadWrite(t *testing.T) {
	srcPath, err := createTempFile()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(srcPath)

	err = fillPath(srcPath, 10000)
	if err != nil {
		t.Fatal(err)
	}

	dstPath, err := createTempFile()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(dstPath)

	cache := blockcache.New(128, 4096)

	st, err := os.Stat(srcPath)
	if err != nil {
		t.Fatal(err)
	}

	srcInode := InodeData{
		Size: uint64(st.Size()),
	}

	srcOverlay, err := OpenROFileOverlay(srcPath, &srcInode, cache, "cas://1234")
	if err != nil {
		t.Fatal(err)
	}
	defer srcOverlay.Close()

	f, err := OpenFileOverlay(srcOverlay, dstPath, 0666, cache)
	if err != nil {
		t.Fatal(err)
	}

	off := 4040
	buf := make([]byte, 100)
	_, err = f.ReadAt(buf, int64(off))
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
	_, err = f.WriteAt(msg, int64(off))
	if err != nil {
		t.Fatal(err)
	}

	off -= 10
	_, err = f.ReadAt(buf, int64(off))
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

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, err = OpenFileOverlay(srcOverlay, dstPath, 0666, cache)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.ReadAt(buf, int64(off))
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
	srcPath, err := createTempFile()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(srcPath)

	err = fillPath(srcPath, 10000)
	if err != nil {
		t.Fatal(err)
	}

	dstPath, err := createTempFile()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(dstPath)

	cache := blockcache.New(128, 4096)

	st, err := os.Stat(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	srcInode := InodeData{
		Size: uint64(st.Size()),
	}

	srcOverlay, err := OpenROFileOverlay(srcPath, &srcInode, cache, "cas://1234")
	if err != nil {
		t.Fatal(err)
	}
	defer srcOverlay.Close()

	f, err := OpenFileOverlay(srcOverlay, dstPath, 0666, cache)
	if err != nil {
		t.Fatal(err)
	}

	off := 9950
	buf := make([]byte, 100)
	n, err := f.ReadAt(buf, int64(off))
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

	f.UpdateInode(func(inodeData *InodeData) error {
		inodeData.Size = 5000
		return nil
	})
	n, err = f.ReadAt(buf, int64(off))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("read unexpected number of bytes, wanted=0 got=%d", n)
	}

	off = 4950
	n, err = f.ReadAt(buf, int64(off))
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
	_, err = f.WriteAt(extraData, 10000)
	if err != nil {
		t.Fatal(err)
	}

	f.UpdateInode(func(inodeData *InodeData) error {
		if inodeData.Size != uint64(10000+len(extraData)) {
			t.Fatal("file size did not increase with write at")
		}
		inodeData.Size = 10000
		return nil
	})
	off = 9950
	n, err = f.ReadAt(buf, int64(off))
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

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, err = OpenFileOverlay(srcOverlay, dstPath, 0666, cache)
	if err != nil {
		t.Fatal(err)
	}

	n, err = f.ReadAt(buf, int64(off))
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

	f.UpdateInode(func(inodeData *InodeData) error {
		inodeData.Size = 20000
		return nil
	})

	n, err = f.ReadAt(buf, int64(off))
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
