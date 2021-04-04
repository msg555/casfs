package testing

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/msg555/ctrfs/fusefs"
)

type TestServer struct {
	Server *fusefs.Server

	tempRoot string
	dataDir  string
	mountDir string
}

func CreateTestServer() (*TestServer, error) {
	tmpDir, err := ioutil.TempDir("", "ctrfs-testing")
	if err != nil {
		return nil, err
	}

	srv := &TestServer{
		tempRoot: tmpDir,
		dataDir:  path.Join(tmpDir, "data"),
		mountDir: path.Join(tmpDir, "mnt"),
	}

	err = os.Mkdir(srv.dataDir, 0700)
	if err != nil {
		return nil, err
	}

	err = os.Mkdir(srv.mountDir, 0700)
	if err != nil {
		return nil, err
	}

	fsrv, err := fusefs.CreateServer(srv.dataDir)
	if err != nil {
		return nil, err
	}

	srv.Server = fsrv
	return srv, nil
}

func (srv *TestServer) Close() error {
	err := srv.Server.Close()
	if err != nil {
		return err
	}

	return os.RemoveAll(srv.tempRoot)
}

func (srv *TestServer) ImportArchive(name string) ([]byte, error) {
	f, err := os.Open(fmt.Sprintf("data/%s.tar.gz", name))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sc, err := srv.Server.Storage.ImportTar(f)
	if err != nil {
		return nil, err
	}

	return sc.NodeAddress[:], nil
}

func (srv *TestServer) Mount(addr []byte) (string, error) {
	err := srv.Server.Mount(srv.mountDir, addr, true)
	if err != nil {
		return "", err
	}
	return srv.mountDir, nil
}
