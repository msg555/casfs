package storage

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os/user"
	"path"

	"github.com/boltdb/bolt"
	"github.com/msg555/casfs/blockfile"
	"github.com/msg555/casfs/btree"
	"github.com/msg555/casfs/castore"
	"github.com/msg555/casfs/unix"
)

const AT_SYMLINK_NOFOLLOW = 0x100
const CONTENT_ADDRESS_LENGTH = 32
const INODE_BUCKET_NAME = "inodes"

type CasfsInode = uint64

type HostInode struct {
	Device uint64
	Inode  uint64
}

type StorageContext struct {
	Cas         *castore.Castore
	HashFactory castore.HashFactory
	InodeBlocks *blockfile.BlockFile
	DirentTree  *btree.BTree
	NodeDB      *bolt.DB
	InodeMap    InodeMap
	BasePath    string
}

type StorageNode struct {
	NodeIndex   blockfile.BlockIndex
	Stat        *InodeData
	NodeAddress [CONTENT_ADDRESS_LENGTH]byte
}

func OpenDefaultStorageContext() (*StorageContext, error) {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}

	return OpenStorageContext(path.Join(usr.HomeDir, ".castore"))
}

func OpenStorageContext(basePath string) (*StorageContext, error) {
	hashFactory := sha256.New
	cas, err := castore.CreateCastore(basePath, hashFactory)
	if err != nil {
		return nil, err
	}

	inodeBlocks, err := blockfile.Open(path.Join(basePath, "inodes.bin"), 0666, INODE_SIZE, false)
	if err != nil {
		return nil, err
	}

	// Choose a fan out to ensure block is under 4KB
	direntTree, err := btree.Open(path.Join(basePath, "dirent.bin"),
		0666, MAX_PATH, DIRENT_SIZE, 14, false)
	if err != nil {
		inodeBlocks.Close()
		log.Fatal(err)
	}

	nodeDB, err := bolt.Open(path.Join(basePath, "contentmap.db"), 0666, nil)
	if err != nil {
		direntTree.Close()
		inodeBlocks.Close()
		log.Fatal(err)
	}

	err = nodeDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(INODE_BUCKET_NAME))
		return err
	})
	if err != nil {
		nodeDB.Close()
		direntTree.Close()
		inodeBlocks.Close()
		log.Fatal(err)
	}

	return &StorageContext{
		HashFactory: hashFactory,
		Cas:         cas,
		InodeBlocks: inodeBlocks,
		DirentTree:  direntTree,
		NodeDB:      nodeDB,
		BasePath:    basePath,
	}, nil
}

func (sc *StorageContext) Close() error {
	if sc.InodeBlocks != nil {
		err := sc.InodeBlocks.Close()
		if err != nil {
			return err
		}
	}
	if sc.DirentTree != nil {
		err := sc.DirentTree.Close()
		if err != nil {
			return err
		}
	}
	if sc.NodeDB != nil {
		err := sc.NodeDB.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *StorageContext) Statfs() (*unix.Statfs_t, error) {
	var buf unix.Statfs_t
	return &buf, unix.Statfs(sc.BasePath, &buf)
}

func (sc *StorageContext) LookupAddressInode(address []byte) (blockfile.BlockIndex, error) {
	var result blockfile.BlockIndex
	err := sc.NodeDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		v := b.Get(address)
		fmt.Println("Lookup", address, v)
		if len(v) == 8 {
			result = bo.Uint64(v)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}

func (sc *StorageContext) ReadInode(nodeIndex blockfile.BlockIndex) (*InodeData, error) {
	buf, err := sc.InodeBlocks.Read(nodeIndex, nil)
	if err != nil {
		return nil, err
	}
	result := InodeData{}
	result.Read(buf)
	return &result, nil
}

func (sc *StorageContext) LookupChild(nd *InodeData, name string) (*Dirent, error) {
	data, _, err := sc.DirentTree.Find(nd.TreeNode, name)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	return direntFromBytes(data), nil
}

func (sc *StorageContext) ScanChildren(nd *InodeData, offset uint64, direntCallback func(offset uint64, name string, ent *Dirent) bool) (bool, error) {
	return sc.DirentTree.Scan(nd.TreeNode, offset, func(offset uint64, key string, val []byte) bool {
		return direntCallback(offset, key, direntFromBytes(val))
	})
}

func (sc *StorageContext) computeNodeAddress(st *InodeData) []byte {
	h := sc.HashFactory()

	var buf [INODE_SIZE]byte
	st.Write(buf[:], true)
	h.Write(buf[:])

	return h.Sum(nil)
}

func NullTerminatedString(data []byte) string {
	for i, ch := range data {
		if ch == 0 {
			return string(data[:i])
		}
	}
	return string(data)
}

type fdReader struct {
	FileDescriptor int
}

func (f fdReader) Read(buf []byte) (int, error) {
	n, err := unix.Read(f.FileDescriptor, buf)
	if err == nil && n == 0 {
		return 0, io.EOF
	}
	return n, err
}
