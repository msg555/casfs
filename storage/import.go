package storage

import (
	"io"
	"reflect"
	"sort"

	"github.com/boltdb/bolt"

	"github.com/msg555/ctrfs/btree"
	"github.com/msg555/ctrfs/unix"
)

type importNode struct {
	Inode         *InodeData
	NodeAddress   [HASH_BYTE_LENGTH]byte
	SeenLocations []importNodeLocation
}

type importNodeLocation struct {
	Path      string
	EdgeIndex btree.IndexType
}

func validatePathName(name string) bool {
	if len(name) > unix.NAME_MAX {
		return false
	}
	if name == "" || name == "." || name == ".." {
		return false
	}
	for _, ch := range name {
		if ch == 0 || ch == '/' {
			return false
		}
	}
	return true
}

func (sc *StorageContext) createHardlinkLayer(rootNode *StorageNode, nodeMap interface{}) (*StorageNode, error) {
	// inodeMap := InodeMap{}
	// inodeMap.Init()
	var linkedNodes [][]importNodeLocation

	for it := reflect.ValueOf(nodeMap).MapRange(); it.Next(); {
		nd := it.Value().Interface().(*importNode)
		if len(nd.SeenLocations) <= 1 {
			continue
		}
		sort.Slice(nd.SeenLocations, func(i, j int) bool {
			return nd.SeenLocations[i].Path < nd.SeenLocations[j].Path
		})
		/*
			TODO
			for _, loc := range nd.SeenLocations[1:] {
				inodeMap.Map[loc.EdgeIndex] = nd.SeenLocations[0].EdgeIndex
			}
		*/
		linkedNodes = append(linkedNodes, nd.SeenLocations)
	}
	if len(linkedNodes) == 0 {
		return rootNode, nil
	}
	sort.Slice(linkedNodes, func(i, j int) bool {
		return linkedNodes[i][0].Path < linkedNodes[j][0].Path
	})

	h := sc.HashFactory()
	h.Write(rootNode.NodeAddress[:])

	var buf [4]byte
	bo.PutUint32(buf[:], uint32(len(linkedNodes)))
	_, err := h.Write(buf[:])
	if err != nil {
		return nil, err
	}
	for _, linkedNode := range linkedNodes {
		bo.PutUint32(buf[:], uint32(len(linkedNode)))
		_, err := h.Write(buf[:])
		if err != nil {
			return nil, err
		}
		for _, loc := range linkedNode {
			io.WriteString(h, loc.Path)
			h.Write([]byte{0})
		}
	}
	// contentAddress := h.Sum(nil)

	/*
		pr, pw := io.Pipe()
		go func() {
			inodeMap.Write(pw)
			pw.Close()
		}()
		hlMapAddress, _, err := sc.Cas.Insert(pr)
		if err != nil {
			return nil, err
		}
	*/

	inode := &InodeData{
		Mode: MODE_HARDLINK_LAYER,
	}
	/*
		copy(inode.PathHash[:], rootNode.NodeAddress[:])
		copy(inode.Address[:], contentAddress)
		copy(inode.XattrAddress[:], hlMapAddress)
	*/

	sn := StorageNode{
		Inode: inode,
	}
	copy(sn.NodeAddress[:], sc.computeNodeAddress(inode))

	err = sc.NodeDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		return b.Put(sn.NodeAddress[:], sn.Inode.ToBytes())
	})
	if err != nil {
		return nil, err
	}

	return &sn, nil
}

func (sc *StorageContext) createDirentTree(nd *importNode, importPath string, children map[string]*importNode, ignoreHardlinks bool) error {
	childPaths := make([]string, 0, len(children))
	for childPath := range children {
		childPaths = append(childPaths, childPath)
	}
	sort.Strings(childPaths)

	h := sc.HashFactory()
	for _, name := range childPaths {
		childNd := children[name]

		io.WriteString(h, name)
		h.Write([]byte{0})
		h.Write(childNd.NodeAddress[:])
	}
	// copy(nd.Inode.Address[:], h.Sum(nil))

	h.Reset()
	io.WriteString(h, importPath)
	// copy(nd.Inode.PathHash[:], h.Sum(nil))

	// copy(nd.NodeAddress[:], sc.computeNodeAddress(nd.Inode))

	err := sc.NodeDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		v := b.Get(nd.NodeAddress[:])
		if len(v) == INODE_SIZE {
			nd.Inode = InodeFromBytes(v)
			return nil
		}

		// Construct Dirent btree
		nd.Inode.Size = 0
		direntMap := make(map[string][]byte)
		for childPath, childNd := range children {
			nd.Inode.Size += childNd.Inode.Size
			direntMap[childPath] = childNd.Inode.ToBytes()
		}
		treeNode, err := sc.DirentTree.WriteRecords(sc, direntMap)
		if err != nil {
			return err
		}

		nd.Inode.TreeNode = treeNode
		return b.Put(nd.NodeAddress[:], nd.Inode.ToBytes())
	})
	if err != nil {
		return nil
	}

	if !ignoreHardlinks {
		for childPath, childNd := range children {
			_, edgeIdx, err := sc.DirentTree.Find(nd.Inode.TreeNode, []byte(childPath))
			if err != nil {
				return err
			}
			childNd.SeenLocations = append(childNd.SeenLocations, importNodeLocation{
				Path:      importPath + "/" + childPath,
				EdgeIndex: edgeIdx,
			})
		}
	}
	return nil
}
