package testing

import (
	"io/ioutil"
	"path"
	"testing"
)

func TestPolling(t *testing.T) {
	srv, err := CreateTestServer()
	if err != nil {
		t.Fatalf("Failed to setup test server: '%s'", err)
	}
	defer func() {
		err := srv.Close()
		if err != nil {
			t.Fatalf("Failed to shutdown server: '%s'", err)
		}
	}()

	addr, err := srv.ImportArchive("basic")
	if err != nil {
		t.Fatalf("Failed to import test archive: '%s'", err)
	}

	mountPoint, err := srv.Mount(addr)
	if err != nil {
		t.Fatalf("Failed to mount: '%s'", err)
	}

	data, err := ioutil.ReadFile(path.Join(mountPoint, "b"))
	if err != nil {
		t.Fatalf("Failed to open file: '%s'", err)
	}
	if len(data) != 0 {
		t.Fatalf("Was expecting empty file")
	}
}
