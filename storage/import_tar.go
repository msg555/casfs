package storage

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/go-errors/errors"

	"github.com/msg555/casfs/blockfile"
	"github.com/msg555/casfs/unix"
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

type TarImportRecord struct {
	Name    string
	Address [HASH_BYTE_LENGTH]byte
	blockfile.BlockIndex
}

func createDirRecord(name string, children []TarImportRecord) TarImportRecord {
	record := TarImportRecord{
		Name: name,
	}
	return record
}

func initInodeDataFromTar(header *tar.Header) (*InodeData, error) {
	var size uint64
	var dev uint64

	mode := header.Mode & 0777
	switch header.Typeflag {
	case tar.TypeReg:
		mode |= unix.S_IFREG
	case tar.TypeSymlink:
		mode |= unix.S_IFLNK
		size = uint64(len(header.Linkname))
	case tar.TypeChar:
		mode |= unix.S_IFCHR
	case tar.TypeBlock:
		mode |= unix.S_IFBLK
	case tar.TypeDir:
		mode |= unix.S_IFDIR
	case tar.TypeFifo:
		mode |= unix.S_IFIFO
	default:
		return nil, errors.New("unexpected object type in archive")
	}

	return &InodeData{
		Mode: uint32(mode),
		Uid:  uint32(header.Uid),
		Gid:  uint32(header.Gid),
		Dev:  dev,
		Atim: uint64(header.AccessTime.Nanosecond()),
		Mtim: uint64(header.ModTime.Nanosecond()),
		Ctim: uint64(header.ChangeTime.Nanosecond()),
		Size: size,
	}, nil
}

func (sc *StorageContext) ImportTar(r io.Reader) (*StorageNode, error) {
	br := bufio.NewReader(r)

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
	var inodeData []*InodeData
	var childrenStack [][]TarImportRecord

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
			// fmt.Println("EMIT RECORD", joinPath(nameStack[:i+1]))
			childrenStack[i-1] = append(childrenStack[i-1], createDirRecord(nameStack[i], childrenStack[i]))
		}
		nameStack = nameStack[:lcp]
		inodeData = inodeData[:lcp]
		childrenStack = childrenStack[:lcp]

		var inode *InodeData
		if record.Typeflag == tar.TypeLink {
			println("HARDLINK", record.Linkname)
			// Need to find hard link
			/*
				linkNameParts := splitPath(record.LinkName)
				linkLcp := calcLcp(linkNameParts)
			*/

		} else {
			inode, err = initInodeDataFromTar(record)
			if err != nil {
				return nil, err
			}
		}

		for i := lcp; i < len(nameParts); i++ {
			if i+1 != len(nameParts) {
				log.Printf("Warning: missing directory entry for '%s'", joinPath(nameParts[:i+1]))
			}
			nameStack = append(nameStack, nameParts[i])
			inodeData = append(inodeData, inode)
			childrenStack = append(childrenStack, nil)
		}
		fmt.Println(record.Name, len(nameParts), nameParts)

		switch record.Typeflag {
		case tar.TypeReg:
			println("Reg file")
		case tar.TypeLink:
			println("link file")
		case tar.TypeSymlink:
			println("symlink", record.Name, record.Linkname)
		case tar.TypeChar:
			println("char file")
		case tar.TypeBlock:
			println("block file")
		case tar.TypeDir:
			println("DIRFILE", record.Name)
		case tar.TypeFifo:
			println("fifo file")

		case tar.TypeGNUSparse:
			log.Fatal("sparse files are not supported")

		default:
			log.Fatalf("Unknown tar type header '%c'", record.Typeflag)
		}

		println(record.Name)
	}
	for i := len(nameStack) - 1; i > 0; i-- {
		// fmt.Println("EMIT RECORD", joinPath(nameStack[:i+1]))
		childrenStack[i-1] = append(childrenStack[i-1], createDirRecord(nameStack[i], childrenStack[i]))
	}

	createDirRecord("", childrenStack[0])
	// fmt.Println(
	//println(root.

	return nil, errors.New("not implemented")
}
