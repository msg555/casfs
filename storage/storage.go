package storage

import (
	"crypto/sha256"
	"io"
	"log"
	"os/user"
	"path"

	"github.com/msg555/ctrfs/btree"
	"github.com/msg555/ctrfs/castore"
	"github.com/msg555/ctrfs/unix"

	"github.com/boltdb/bolt"
)

const DIRENT_BTREE_FANOUT = 9
const HASH_BYTE_LENGTH = 32
const INODE_BUCKET_NAME = "inodes"

type InodeId = btree.IndexType

type StorageContext struct {
	Cas         *castore.Castore
	HashFactory castore.HashFactory
	DirentTree  btree.BTree
	NodeDB      *bolt.DB
	BasePath    string
}

type StorageNode struct {
	Inode       *InodeData
	NodeAddress [HASH_BYTE_LENGTH]byte
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
	cas, err := castore.CreateCastore(path.Join(basePath, "cas"), hashFactory)
	if err != nil {
		return nil, err
	}

	nodeDB, err := bolt.Open(path.Join(basePath, "contentmap.db"), 0666, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = nodeDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(INODE_BUCKET_NAME))
		return err
	})
	if err != nil {
		nodeDB.Close()
		log.Fatal(err)
	}

	sc := &StorageContext{
		HashFactory: hashFactory,
		Cas:         cas,
		NodeDB:      nodeDB,
		BasePath:    basePath,
	}

	// Choose a fan out to ensure block is under 4KB
	err = sc.DirentTree.Open(path.Join(basePath, "dirent.bin"),
		0666, unix.NAME_MAX, INODE_SIZE, DIRENT_BTREE_FANOUT, false)
	if err != nil {
		sc.Close()
		return nil, err
	}

	return sc, nil
}

func (sc *StorageContext) Close() error {
	err := sc.DirentTree.Close()
	if err != nil {
		return err
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

func (sc *StorageContext) LookupAddressInode(address []byte) (*InodeData, error) {
	var result *InodeData
	err := sc.NodeDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		v := b.Get(address)
		if len(v) == INODE_SIZE {
			result = inodeFromBytes(v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (sc *StorageContext) ReadInode(nodeIndex InodeId) (*InodeData, error) {
	buf, err := sc.DirentTree.ByIndex(nodeIndex)
	if err != nil {
		return nil, err
	}
	return inodeFromBytes(buf), nil
}

func (sc *StorageContext) LookupChild(nd *InodeData, name string) (*InodeData, InodeId, error) {
	data, childId, err := sc.DirentTree.Find(nd.TreeNode, name)
	if err != nil {
		return nil, 0, err
	}
	if data == nil {
		return nil, 0, nil
	}
	return inodeFromBytes(data), childId, nil
}

func (sc *StorageContext) ScanChildren(nd *InodeData, offset uint64,
	direntCallback func(offset uint64, inodeId InodeId, name string, ent *InodeData) bool) (bool, error) {
	return sc.DirentTree.Scan(nd.TreeNode, offset, func(offset uint64, index btree.IndexType, key string, val []byte) bool {
		return direntCallback(offset, index, key, inodeFromBytes(val))
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
