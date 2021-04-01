package storage

import (
	"io"
	"sort"

	"github.com/boltdb/bolt"
)

func (dc *dirImportContext) createHardlinkLayer(rootNode *StorageNode) (*StorageNode, error) {
	dc.Storage.InodeMap.Init()
	var linkedNodes [][]dirImportNodeLocation
	for _, nd := range dc.InodeMap {
		if len(nd.SeenLocations) <= 1 {
			continue
		}
		sort.Slice(nd.SeenLocations, func(i, j int) bool {
			return nd.SeenLocations[i].Path < nd.SeenLocations[j].Path
		})
		for _, loc := range nd.SeenLocations[1:] {
			dc.Storage.InodeMap.Map[loc.EdgeIndex] = nd.SeenLocations[0].EdgeIndex
		}
		linkedNodes = append(linkedNodes, nd.SeenLocations)
	}
	sort.Slice(linkedNodes, func(i, j int) bool {
		return linkedNodes[i][0].Path < linkedNodes[j][0].Path
	})

	h := dc.Storage.HashFactory()
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
	contentAddress := h.Sum(nil)

	pr, pw := io.Pipe()
	go func() {
		dc.Storage.InodeMap.Write(pw)
		pw.Close()
	}()
	hlMapAddress, _, err := dc.Storage.Cas.Insert(pr)
	if err != nil {
		return nil, err
	}

	inode := &InodeData{
		Mode:     MODE_HARDLINK_LAYER,
	}
	copy(inode.PathHash[:], rootNode.NodeAddress[:])
	copy(inode.Address[:], contentAddress)
	copy(inode.XattrAddress[:], hlMapAddress)

	sc := StorageNode{
		Inode: inode,
	}
	copy(sc.NodeAddress[:], dc.Storage.computeNodeAddress(inode))

	err = dc.Storage.NodeDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(INODE_BUCKET_NAME))
		return b.Put(sc.NodeAddress[:], sc.Inode.toBytes())
	})
	if err != nil {
		return nil, err
	}

	return &sc, nil
}
