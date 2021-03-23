// Implements a simple, immutable B-tree implementation on disk featuring fixed
// length keys and values. Keys are strings that have variable length not
// exceeding MaxKeyLength. Values are fixed length byte arrays of size
// RecordLength.
// Has a simple API for constructing the B-tree all at once or by writing
// it record by record in sorted order.

package main

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/msg555/casfs"
)

const ALIGNMENT = 128

type KeyType = string
type ValueType = []byte

type KeyValuePair struct {
	Key KeyType
	Value ValueType
}

// Represents a B-tree structure. Set the public variables then call Init().
// You can then start querying or writing records to the tree.
type BTree struct {
	// Maximum length in bytes of a key
	MaxKeyLength	int

	// Length in bytes of a value
	RecordLength	int

	// The number of keys stored at each B-tree node. Each node has up to FanOut+1
	// child nodes.
	FanOut				int

	// Total number of keys in the tree
	Size					int

	// File must be opened for reading if querying the B-tree and writing if
	// constructing the B-tree.
	File					*os.File

	// Offset to write btree in the file
	FileOffset		int64

	// Precomputed array of number of nodes in a full tree of a certain depth
	depthSizes		[]int
	// Maximum depth of any node in the tree
	maxDepth			int
	// Last sort index that appears at the maximum depth of the tree
	lastDeepNode	int
	// Number of bytes in total for a record
	nodeSize			int
}

// Initialize the private variables of a btree
func (tr *BTree) Init() {
	tr.depthSizes = []int{0}
	tr.maxDepth = 0
	for tr.depthSizes[tr.maxDepth] < tr.Size {
		tr.depthSizes = append(tr.depthSizes, tr.depthSizes[tr.maxDepth] * (tr.FanOut + 1) + tr.FanOut)
		tr.maxDepth++
	}
	tr.nodeSize = 4 + tr.MaxKeyLength + tr.RecordLength

	tr.lastDeepNode = tr.Size - tr.depthSizes[tr.maxDepth - 1] - 1
	tr.lastDeepNode += tr.lastDeepNode / tr.FanOut
}

// Total size in bytes of the B-tree
func (tr* BTree) SizeOnDisk() int64 {
	return int64(tr.Size) * int64(tr.nodeSize)
}

// Writes a single record to the B-tree. sortIndex gives the index of the
// key if all the keys were listed in sorted order. The records may be written
// in any order but the caller is responsible for ensuring that the sort
// condition is made.
func (tr *BTree) WriteRecord(sortIndex int, key KeyType, value ValueType) error {
	nodeIndex := tr.nodeIndex(sortIndex)

	nodeData, err := tr.makeNode(key, value)
	if err != nil {
		return err
	}

	written := 0
	for written < len(nodeData) {
		n, err := tr.File.WriteAt(nodeData[written:], tr.FileOffset + int64(nodeIndex) * int64(tr.nodeSize) + int64(written))
		if err != nil {
			return err
		}
		written += n
	}

	return nil
}

func (tr *BTree) WriteRecords(data map[KeyType]ValueType) error {
	keys := make([]KeyType, len(data))

	i := 0
	for key := range data {
		keys[i] = key
		i++
	}
	sort.Strings(keys)

	// Write records in disk order
	for i := 0; i < tr.Size; i++ {
		si := tr.sortIndex(i)
		key := keys[si]
		err := tr.WriteRecord(si, key, data[key])
		if err != nil {
			return err
		}
	}

	return nil
}

// Make a node byte data for writing
func (tr *BTree) makeNode(key KeyType, value ValueType) ([]byte, error) {
	if tr.MaxKeyLength < len(key) {
		return nil, errors.New("key length too long")
	}
	if tr.RecordLength != len(value) {
		return nil, errors.New("record length incorrect")
	}

	nodeData := make([]byte, tr.nodeSize)
	casfs.Hbo.PutUint32(nodeData, uint32(len(key)))
	copy(nodeData[4:], key)
	copy(nodeData[4+tr.MaxKeyLength:], value)
	return nodeData, nil
}

func (tr *BTree) Find(key string) ([]byte, error) {
	blockRaw := make([]byte, tr.FanOut * tr.nodeSize)

	depth := 1
	depthIndex := 0
	for {
		nodeIndex := tr.depthSizes[depth - 1] + depthIndex
		if nodeIndex >= tr.Size {
			break
		}

		blockNodes := tr.FanOut
		if nodeIndex + blockNodes > tr.Size {
			blockNodes = tr.Size - nodeIndex
		}
		block := blockRaw[:blockNodes * tr.nodeSize]

		read := 0
		for read < len(block) {
			n, err := tr.File.ReadAt(block[read:], tr.FileOffset + int64(nodeIndex) * int64(tr.nodeSize) + int64(read))
			if err != nil {
				return nil, err
			}
			read += n
		}

		lo := 0
		hi := blockNodes - 1
		for {
			md := lo + (hi - lo) / 2
			nodeData := block[md * tr.nodeSize:(md + 1) * tr.nodeSize]

			keylen := int(casfs.Hbo.Uint32(nodeData))
			if keylen > tr.MaxKeyLength {
				return nil, errors.New("unexpected long key length")
			}

			cmp := strings.Compare(key, string(nodeData[4:4+keylen]))
			if cmp == 0 {
				// copy this?
				return nodeData[4+tr.MaxKeyLength:], nil
			} else if cmp == -1 {
				hi = md - 1
			} else {
				lo = md + 1
			}
			if hi < lo {
				depthIndex = depthIndex / (tr.FanOut) * (tr.FanOut + 1) + md
				if cmp == 1 {
					depthIndex += 1
				}
				depthIndex *= tr.FanOut
				depth++
				break
			}
		}
	}

	return nil, nil
}

// Converts a sort index into a node index within the btree.
func (tr *BTree) nodeIndex(sortIndex int) int {
	var level int
	if sortIndex <= tr.lastDeepNode {
		level = tr.maxDepth
	} else {
		level = tr.maxDepth - 1
		sortIndex = sortIndex - tr.lastDeepNode - 1 + tr.lastDeepNode / (tr.FanOut + 1)
	}

	for (sortIndex + 1) % (tr.FanOut + 1) == 0 {
		level--
		sortIndex /= tr.FanOut + 1
	}

	return tr.depthSizes[level - 1] + sortIndex - sortIndex / (tr.FanOut + 1)
}

// Converts a node index into a sort index within the btree.
func (tr *BTree) sortIndex(nodeIndex int) int {
	level := 0
	for tr.depthSizes[level] <= nodeIndex {
		level++
	}
	levelPos := nodeIndex - tr.depthSizes[level - 1]
	levelPos += levelPos / tr.FanOut

	for i := level; i + 1 < tr.maxDepth; i++ {
		levelPos = (levelPos + 1) * (tr.FanOut + 1) - 1
	}
	if level != tr.maxDepth {
		lastDepth := tr.Size - tr.depthSizes[tr.maxDepth - 1]

		newPos := (levelPos + 1) * (tr.FanOut + 1) - 1

		levelPos += lastDepth
		if newPos < levelPos {
			levelPos = newPos
		}
	}

	return levelPos
}


func main() {
	//file, err := os.Create("test.btree")
	file, err := os.Open("test.btree")
	if err != nil {
		panic(err)
	}

	tr := BTree{
		MaxKeyLength: 16,
		RecordLength: 16,
		FanOut: 3,
		Size: 44,
		File: file,
	}
	tr.Init()

/*
	data := make(map[string][]byte)
	for i := 0; i < tr.Size; i++ {
		data[fmt.Sprintf("wow-%d", i)] = []byte(fmt.Sprintf("%16d", i))
	}

	err = tr.WriteRecords(data)
	if err != nil {
		panic(err)
	}
*/

	for i := 0; i < tr.Size; i++ {
		data, err := tr.Find(fmt.Sprintf("wow-%d", i))
		if err != nil {
			panic(err)
		}

		if data == nil {
			fmt.Println("No data found")
			panic("fail")
		} else {
			fmt.Println("Got data:", data)
		}
	}

	for i := 0; i < tr.Size; i++ {
		ni := tr.nodeIndex(i)
		si := tr.sortIndex(ni)
		fmt.Println("index:", i, ni, si)
		if i != si {
			panic("FAIL")
		}
	}
}
