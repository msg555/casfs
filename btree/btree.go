// Implements a simple, immutable B-tree implementation on disk featuring fixed
// length keys and values. Keys are strings that have variable length not
// exceeding MaxKeyLength. Values are fixed length byte arrays of size
// RecordLength.
// Has a simple API for constructing the B-tree all at once or by writing
// it record by record in sorted order.

package btree

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/msg555/casfs"
	"github.com/msg555/casfs/blockfile"
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
	// Maximum size in bytes of a key
	MaxKeySize		int

	// Size in bytes of a value
	EntrySize			int

	// The number of keys stored at each B-tree node. Each node has up to FanOut+1
	// child nodes.
	FanOut				int

	// Number of bytes in total for an entry
	nodeSize			int

	// Underlying block store to keep b-tree nodes
	blocks				*blockfile.BlockFile
}

func Open(path string, perm os.FileMode, maxKeySize, entrySize, fanOut int, readOnly bool) (*BTree, error) {
	nodeSize := 4 + maxKeySize + entrySize
	blockSize := 8 * (fanOut + 1) + nodeSize * fanOut
	blocks, err := blockfile.Open(path, perm, blockSize, readOnly)
	if err != nil {
		return nil, err
	}
	fmt.Println("BLOCKSIZE:", blockSize)

	return &BTree{
		MaxKeySize: maxKeySize,
		EntrySize: entrySize,
		FanOut: fanOut,
		nodeSize: nodeSize,
		blocks: blocks,
	}, nil
}

func (tr *BTree) Close() error {
	return tr.blocks.Close()
}

func (tr *BTree) WriteRecords(data map[KeyType]ValueType) (blockfile.BlockIndex, error) {
	size := len(data)
	if size == 0 {
		return 0, nil
	}

	type KVPair struct {
		Key		KeyType
		Value	ValueType
	}

	i := 0
	sortedData := make([]KVPair, len(data))
	for key, value := range data {
		if len(key) > int(tr.MaxKeySize) {
			return 0, errors.New("key length too long")
		}
		if len(value) != int(tr.EntrySize) {
			return 0, errors.New("value wrong length")
		}
		sortedData[i] = KVPair{
			Key: key,
			Value: value,
		}
		i++
	}
	sort.Slice(sortedData, func(i, j int) bool {
		return sortedData[i].Key < sortedData[j].Key
	})

	var err error
	blocks := make([]blockfile.BlockIndex, (size + tr.FanOut - 1) / tr.FanOut)
	for i = 0; i < len(blocks); i++ {
		blocks[i], err = tr.blocks.Allocate()
		if err != nil {
			return 0, err
		}
	}

	nextChild := 1
	idxer := createIndexer(tr.FanOut, size)
	buf := make([]byte, tr.blocks.BlockSize)
	for i = 0; i < len(blocks); i++ {
		// Zero buffer
		for j := range buf {
			buf[j] = 0
		}

		// Write child block indexes
		for j := 0; j <= tr.FanOut && nextChild < len(blocks); j++ {
			casfs.Hbo.PutUint64(buf[j * 8:], blocks[nextChild])
			nextChild++
		}
		recordBuf := buf[8 * (tr.FanOut + 1):]

		// Write node data
		for j := i * tr.FanOut; j < (i + 1) * tr.FanOut && j < size; j++ {
			si := idxer.sortIndex(j)
			kvPair := sortedData[si]

			casfs.Hbo.PutUint32(recordBuf, uint32(len(kvPair.Key)))
			copy(recordBuf[4:], kvPair.Key)
			copy(recordBuf[4+tr.MaxKeySize:], kvPair.Value)
			recordBuf = recordBuf[tr.nodeSize:]
		}

		err = tr.blocks.Write(blocks[i], buf)
		if err != nil {
			return 0, err
		}
	}

	return blocks[0], nil
}

func (tr *BTree) Find(nodeIndex blockfile.BlockIndex, key string) ([]byte, error) {
	// Index 0 means block doesn't exist
	block := make([]byte, tr.blocks.BlockSize)

	for nodeIndex != 0 {
		_, err := tr.blocks.Read(nodeIndex, block)
		if err != nil {
			return nil, err
		}

		lo := 0
		hi := tr.FanOut - 1
		for {
			md := lo + (hi - lo) / 2
			nodeData := block[8 * (tr.FanOut + 1) + md * tr.nodeSize:]

			keylen := int(casfs.Hbo.Uint32(nodeData))
			if keylen > tr.MaxKeySize {
				return nil, errors.New("unexpected long key length")
			}
			if keylen == 0 {
				hi = md -1
				continue
			}

			cmp := strings.Compare(key, string(nodeData[4:4+keylen]))
			if cmp == 0 {
				// copy this?
				return nodeData[4+tr.MaxKeySize:4+tr.MaxKeySize+tr.EntrySize], nil
			} else if cmp == -1 {
				hi = md - 1
			} else {
				lo = md + 1
			}
			if hi < lo {
				if cmp == 1 {
					md++
				}
				nodeIndex = casfs.Hbo.Uint64(block[8*md:])
				break
			}
		}
	}

	return nil, nil
}

type treeIndexer struct {
	fanOut				int
	size					int
	depthSizes		[]int
	maxDepth			int
	lastDeepNode	int
}

func createIndexer(fanOut int, size int) treeIndexer {
	idx := treeIndexer {
		fanOut: fanOut,
		size: size,
		depthSizes: []int{0},
	}

	for idx.depthSizes[idx.maxDepth] < idx.size {
		idx.depthSizes = append(idx.depthSizes, idx.depthSizes[idx.maxDepth] * (idx.fanOut + 1) + idx.fanOut)
		idx.maxDepth++
	}

	idx.lastDeepNode = idx.size - idx.depthSizes[idx.maxDepth - 1] - 1
	idx.lastDeepNode += idx.lastDeepNode / idx.fanOut
	return idx
}

// Converts a sort index into a node index within the btree.
func (idx treeIndexer) nodeIndex(sortIndex int) int {
	var level int
	if sortIndex <= idx.lastDeepNode {
		level = idx.maxDepth
	} else {
		level = idx.maxDepth - 1
		sortIndex = sortIndex - idx.lastDeepNode - 1 + idx.lastDeepNode / (idx.fanOut + 1)
	}

	for (sortIndex + 1) % (idx.fanOut + 1) == 0 {
		level--
		sortIndex /= idx.fanOut + 1
	}

	return idx.depthSizes[level - 1] + sortIndex - sortIndex / (idx.fanOut + 1)
}

// Converts a node index into a sort index within the btree.
func (idx treeIndexer) sortIndex(nodeIndex int) int {
	level := 0
	for idx.depthSizes[level] <= nodeIndex {
		level++
	}
	levelPos := nodeIndex - idx.depthSizes[level - 1]
	levelPos += levelPos / idx.fanOut

	for i := level; i + 1 < idx.maxDepth; i++ {
		levelPos = (levelPos + 1) * (idx.fanOut + 1) - 1
	}
	if level != idx.maxDepth {
		lastDepth := idx.size - idx.depthSizes[idx.maxDepth - 1]

		newPos := (levelPos + 1) * (idx.fanOut + 1) - 1

		levelPos += lastDepth
		if newPos < levelPos {
			levelPos = newPos
		}
	}

	return levelPos
}


func main() {
	tr, err := Open("test.btree", 0666, 16, 16, 3, false)
	if err != nil {
		panic(err)
	}

	size := 16

	data := make(map[string][]byte)
	for i := 0; i < size; i++ {
		data[fmt.Sprintf("wow-%d", i)] = []byte(fmt.Sprintf("%16d", i))
	}

	rootIndex, err := tr.WriteRecords(data)
	if err != nil {
		panic(err)
	}

	fmt.Println("Wrote at index", rootIndex)

	for i := 0; i < size; i++ {
		data, err := tr.Find(rootIndex, fmt.Sprintf("wow-%d", i))
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

	fmt.Println("hello")
}
