package storage

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"io"
	"log"
	"strings"

	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/unix"
)

func splitPath(path string) []string {
	result := []string{""}

	validate := func(part string) bool {
		return part != "" && part != "."
	}

	last_i := 0
	for i, ch := range path {
		if ch == '/' {
			part := path[last_i:i]
			last_i = i + 1
			if validate(part) {
				result = append(result, part)
			}
		}
	}

	part := path[last_i:]
	if validate(part) {
		result = append(result, part)
	}
	return result
}

func joinPath(path []string) string {
	if len(path) <= 1 {
		return "/"
	}
	return strings.Join(path, "/")
}

func calcLcp(A []string, B []string) int {
	i := 0
	for i < len(A) && i < len(B) && A[i] == B[i] {
		i++
	}
	return i
}

func initNodeFromTar(header *tar.Header) (*InodeData, error) {
	var dev uint64
	var err error

	mode := header.Mode & 07777
	switch header.Typeflag {
	case tar.TypeReg:
		mode |= unix.S_IFREG
	case tar.TypeSymlink:
		mode |= unix.S_IFLNK
	case tar.TypeChar:
		mode |= unix.S_IFCHR
		dev, err = unix.Makedev(uint64(header.Devmajor), uint64(header.Devminor))
		if err != nil {
			return nil, err
		}
	case tar.TypeBlock:
		mode |= unix.S_IFBLK
		dev, err = unix.Makedev(uint64(header.Devmajor), uint64(header.Devminor))
		if err != nil {
			return nil, err
		}
	case tar.TypeDir:
		mode |= unix.S_IFDIR
	case tar.TypeFifo:
		mode |= unix.S_IFIFO
	default:
		return nil, errors.New("unsupported object type in archive")
	}

	// TreeNode, PathHash, Address, XattrAddress
	return &InodeData{
		Mode: uint32(mode),
		Uid:  uint32(header.Uid),
		Gid:  uint32(header.Gid),
		Dev:  dev,
		Atim: uint64(header.AccessTime.Nanosecond()),
		Mtim: uint64(header.ModTime.Nanosecond()),
		Ctim: uint64(header.ChangeTime.Nanosecond()),
	}, nil
}

func createMissingDirNode(fromNd *importNode) *importNode {
	return &importNode{
		Inode: &InodeData{
			Mode: (fromNd.Inode.Mode & 0777) | unix.S_IFDIR,
			Uid:  fromNd.Inode.Uid,
			Gid:  fromNd.Inode.Gid,
			Atim: fromNd.Inode.Atim,
			Mtim: fromNd.Inode.Mtim,
			Ctim: fromNd.Inode.Ctim,
		},
	}
}

func tarFinalizeNode(sc *StorageContext, nd *importNode, importPath string, children map[string]*importNode, ignoreHardlinks bool) error {
	if !unix.S_ISDIR(nd.Inode.Mode) {
		return nil
	}

	return sc.createDirentTree(nd, importPath, children, ignoreHardlinks)
}

func (sc *StorageContext) ImportTar(r io.Reader) (*StorageNode, error) {
	br := bufio.NewReader(r)

	ignoreHardlinks := false

	fileHeader, err := br.Peek(2)
	if err != nil {
		return nil, err
	}

	// Check for gzip magic header, decompress stream if needed.
	r = br
	if fileHeader[0] == 0x1F && fileHeader[1] == 0x8B {
		gzr, err := gzip.NewReader(br)
		if err == nil {
			r = gzr
		}
	}

	var nameStack []string
	var nodeStack []*importNode
	var childMapStack []map[string]*importNode
	fileNodeMap := make(map[string]*importNode)

	arch := tar.NewReader(r)
	for {
		record, err := arch.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		nameParts := splitPath(record.Name)

		lcp := calcLcp(nameStack, nameParts)
		for i := len(nameStack) - 1; i >= lcp; i-- {
			dirNd := nodeStack[i]
			err = tarFinalizeNode(sc, dirNd, strings.Join(nameStack[:i+1], "/"), childMapStack[i], ignoreHardlinks)
			if err != nil {
				return nil, err
			}
		}
		nameStack = nameStack[:lcp]
		nodeStack = nodeStack[:lcp]
		childMapStack = childMapStack[:lcp]

		var nd *importNode
		if record.Typeflag == tar.TypeLink {
			nd = fileNodeMap[record.Linkname]
			if nd == nil {
				return nil, errors.New("hardlink references non-existant file")
			}
		} else {
			inode, err := initNodeFromTar(record)
			if err != nil {
				return nil, err
			}

			switch record.Typeflag {
			case tar.TypeSymlink:
				addr, size, err := sc.Cas.Insert(strings.NewReader(record.Linkname))
				if err != nil {
					return nil, err
				}
				inode.Size = uint64(size)
				copy(inode.Address[:], addr)
			case tar.TypeReg:
				addr, size, err := sc.Cas.Insert(arch)
				if err != nil {
					return nil, err
				}
				inode.Size = uint64(size)
				copy(inode.Address[:], addr)
			}

			nd = &importNode{
				Inode: inode,
			}
			if record.Typeflag != tar.TypeDir {
				fileNodeMap[record.Name] = nd
				copy(nd.NodeAddress[:], sc.computeNodeAddress(nd.Inode))
			}
		}

		for i := lcp; i < len(nameParts); i++ {
			addNd := nd
			if i+1 != len(nameParts) {
				log.Printf("Warning: missing directory entry for '%s'", joinPath(nameParts[:i+1]))
				addNd = createMissingDirNode(nd)
			}
			if i > 0 {
				if _, exists := childMapStack[i-1][nameParts[i]]; exists {
					log.Printf("Warning: duplicate entry at '%s', using later entry", joinPath(nameParts[:i+1]))
				}
				childMapStack[i-1][nameParts[i]] = addNd
			}

			nameStack = append(nameStack, nameParts[i])
			nodeStack = append(nodeStack, addNd)
			childMapStack = append(childMapStack, make(map[string]*importNode))
		}
	}
	for i := len(nameStack) - 1; i > 0; i-- {
		dirNd := nodeStack[i]
		err = tarFinalizeNode(sc, dirNd, strings.Join(nameStack[:i+1], "/"), childMapStack[i], ignoreHardlinks)
		if err != nil {
			return nil, err
		}
	}
	if len(nodeStack) == 0 {
		return nil, errors.New("empty archive")
	}

	err = tarFinalizeNode(sc, nodeStack[0], "", childMapStack[0], ignoreHardlinks)
	if err != nil {
		return nil, err
	}

	rootNode := &StorageNode{
		Inode:       nodeStack[0].Inode,
		NodeAddress: nodeStack[0].NodeAddress,
	}
	if ignoreHardlinks {
		return rootNode, nil
	}
	return sc.createHardlinkLayer(rootNode, fileNodeMap)
}
