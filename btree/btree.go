// Implements a simple, immutable B-tree implementation on disk featuring fixed
// length keys and values. Keys are strings that have variable length not
// exceeding MaxKeyLength. Values are fixed length byte arrays of size
// RecordLength.
// Has a simple API for constructing the B-tree all at once or by writing
// it record by record in sorted order.

package btree

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/go-errors/errors"

	"github.com/msg555/casfs/blockfile"
)

var bo = binary.LittleEndian

const ALIGNMENT = 128

type KeyType = string
type ValueType = []byte
type IndexType = uint64

type KeyValuePair struct {
	Key   KeyType
	Value ValueType
}

// Represents a B-tree structure. Set the public variables then call Init().
// You can then start querying or writing records to the tree.
type BTree struct {
	// Maximum size in bytes of a key
	MaxKeySize int

	// Size in bytes of a value
	EntrySize int

	// The number of keys stored at each B-tree node. Each node has up to FanOut+1
	// child nodes.
	FanOut int

	// Number of bytes in total for an entry
	nodeSize int

	// Underlying block store to keep b-tree nodes
	blocks blockfile.BlockFile
}

func (tr *BTree) Open(path string, perm os.FileMode, maxKeySize, entrySize, fanOut int, readOnly bool) error {
	nodeSize := 4 + maxKeySize + entrySize
	blockSize := 8*(fanOut+1) + nodeSize*fanOut
	err := tr.blocks.Open(path, perm, blockSize, readOnly)
	if err != nil {
		return err
	}

	tr.nodeSize = nodeSize
	tr.MaxKeySize = maxKeySize
	tr.EntrySize = entrySize
	tr.FanOut = fanOut
	return nil
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
		Key   KeyType
		Value ValueType
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
			Key:   key,
			Value: value,
		}
		i++
	}
	sort.Slice(sortedData, func(i, j int) bool {
		return sortedData[i].Key < sortedData[j].Key
	})

	var err error
	blocks := make([]blockfile.BlockIndex, (size+tr.FanOut-1)/tr.FanOut)
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
			bo.PutUint64(buf[j*8:], blocks[nextChild])
			nextChild++
		}
		recordBuf := buf[8*(tr.FanOut+1):]

		// Write node data
		for j := i * tr.FanOut; j < (i+1)*tr.FanOut && j < size; j++ {
			si := idxer.sortIndex(j)
			kvPair := sortedData[si]

			bo.PutUint32(recordBuf, uint32(len(kvPair.Key)))
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

// Start scanning entries at the given offset (pass 0 to start at the beginning).
// Scan() will invoke entryCallback
// for each entry. If entryCallback returns false the scan will terminate.
// A scan can be resumed starting at a given entry by passing back the offset
// parameter sent to the callback function.
// If the scan finishes processing all records it will return 0. Otherwise
// it will return an opaque offset that can be passed back to Scan() to resume
// the scan starting with the record that entryCallback returned false on.
// Scan() will return true if the scan has reached the end of the btree.
func (tr *BTree) Scan(nodeIndex blockfile.BlockIndex, offset uint64, entryCallback func(offset uint64, index IndexType, key KeyType, value ValueType) bool) (bool, error) {
	if nodeIndex == 0 {
		return true, nil
	}

	var stackBlocks [][]byte
	var stackBlockIndexes []blockfile.BlockIndex
	var stackIndexes []int
	for {
		block := make([]byte, tr.blocks.BlockSize)
		_, err := tr.blocks.Read(nodeIndex, block)
		if err != nil {
			return false, err
		}

		childIndex := offset % uint64(tr.FanOut+1)
		stackBlocks = append(stackBlocks, block)
		stackBlockIndexes = append(stackBlockIndexes, nodeIndex)
		stackIndexes = append(stackIndexes, int(childIndex))
		if offset <= uint64(tr.FanOut) {
			break
		}

		offset /= uint64(tr.FanOut + 1)
		nodeIndex = blockfile.BlockIndex(bo.Uint32(block[8*childIndex:]))
	}

	moveUp := true
	stackDepth := len(stackIndexes) - 1
	if stackIndexes[stackDepth] > 0 {
		stackIndexes[stackDepth]--
		moveUp = false
	}
	for {
		if moveUp {
			for {
				childIndex := bo.Uint64(stackBlocks[stackDepth][8*stackIndexes[stackDepth]:])
				if childIndex == 0 {
					break
				}

				block := make([]byte, tr.blocks.BlockSize)
				_, err := tr.blocks.Read(childIndex, block)
				if err != nil {
					return false, err
				}

				stackBlocks = append(stackBlocks, block)
				stackBlockIndexes = append(stackBlockIndexes, childIndex)
				stackIndexes = append(stackIndexes, 0)
				stackDepth++
			}
		}
		moveUp = true

		for {
			if stackDepth == -1 {
				return true, nil
			}
			block := stackBlocks[stackDepth]
			index := stackIndexes[stackDepth]
			if index == tr.FanOut || bo.Uint64(block[8*(tr.FanOut+1)+index*tr.nodeSize:]) == 0 {
				stackDepth--
			} else {
				break
			}
		}

		stackBlocks = stackBlocks[:stackDepth+1]
		stackBlockIndexes = stackBlockIndexes[:stackDepth+1]
		stackIndexes = stackIndexes[:stackDepth+1]

		block := stackBlocks[stackDepth]
		blockIndex := stackBlockIndexes[stackDepth]
		index := stackIndexes[stackDepth]
		nodeData := block[8*(tr.FanOut+1)+index*tr.nodeSize:]

		keylen := int(bo.Uint32(nodeData))
		if keylen > tr.MaxKeySize {
			return false, errors.New("unexpected long key length")
		}

		offset = uint64(index + 1)
		for i := stackDepth - 1; i >= 0; i-- {
			offset = offset*uint64(tr.FanOut+1) + uint64(stackIndexes[i])
		}
		if !entryCallback(offset, blockIndex*uint64(tr.FanOut)+uint64(index), string(nodeData[4:4+keylen]), nodeData[4+tr.MaxKeySize:4+tr.MaxKeySize+tr.EntrySize]) {
			return false, nil
		}

		stackIndexes[stackDepth]++
	}
}

func (tr *BTree) Find(nodeIndex blockfile.BlockIndex, key KeyType) (ValueType, IndexType, error) {
	// Index 0 means block doesn't exist
	block := make([]byte, tr.blocks.BlockSize)

	for nodeIndex != 0 {
		_, err := tr.blocks.Read(nodeIndex, block)
		if err != nil {
			return nil, 0, err
		}

		lo := 0
		hi := tr.FanOut - 1
		for {
			md := lo + (hi-lo)/2
			nodeData := block[8*(tr.FanOut+1)+md*tr.nodeSize:]

			keylen := int(bo.Uint32(nodeData))
			if keylen > tr.MaxKeySize {
				return nil, 0, errors.New("unexpected long key length")
			}
			if keylen == 0 {
				hi = md - 1
				if hi < lo {
					nodeIndex = bo.Uint64(block[8*md:])
					break
				}
				continue
			}

			cmp := strings.Compare(key, string(nodeData[4:4+keylen]))
			if cmp == 0 {
				// copy this?
				return nodeData[4+tr.MaxKeySize : 4+tr.MaxKeySize+tr.EntrySize], nodeIndex*uint64(tr.FanOut) + uint64(md), nil
			} else if cmp == -1 {
				hi = md - 1
			} else {
				lo = md + 1
			}
			if hi < lo {
				if cmp == 1 {
					md++
				}
				nodeIndex = bo.Uint64(block[8*md:])
				break
			}
		}
	}

	return nil, 0, nil
}

func (tr *BTree) ByIndex(index IndexType) (ValueType, error) {
	nodeIndex := blockfile.BlockIndex(index / uint64(tr.FanOut))
	pos := int(index % uint64(tr.FanOut))
	return tr.blocks.ReadAt(nodeIndex, 8*(tr.FanOut+1)+pos*tr.nodeSize+4+tr.MaxKeySize, tr.EntrySize, nil)
}

type treeIndexer struct {
	fanOut       int
	size         int
	depthSizes   []int
	maxDepth     int
	lastDeepNode int
}

func createIndexer(fanOut int, size int) treeIndexer {
	idx := treeIndexer{
		fanOut:     fanOut,
		size:       size,
		depthSizes: []int{0},
	}

	for idx.depthSizes[idx.maxDepth] < idx.size {
		idx.depthSizes = append(idx.depthSizes, idx.depthSizes[idx.maxDepth]*(idx.fanOut+1)+idx.fanOut)
		idx.maxDepth++
	}

	idx.lastDeepNode = idx.size - idx.depthSizes[idx.maxDepth-1] - 1
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
		sortIndex = sortIndex - idx.lastDeepNode - 1 + idx.lastDeepNode/(idx.fanOut+1)
	}

	for (sortIndex+1)%(idx.fanOut+1) == 0 {
		level--
		sortIndex /= idx.fanOut + 1
	}

	return idx.depthSizes[level-1] + sortIndex - sortIndex/(idx.fanOut+1)
}

// Converts a node index into a sort index within the btree.
func (idx treeIndexer) sortIndex(nodeIndex int) int {
	level := 0
	for idx.depthSizes[level] <= nodeIndex {
		level++
	}
	levelPos := nodeIndex - idx.depthSizes[level-1]
	levelPos += levelPos / idx.fanOut

	for i := level; i+1 < idx.maxDepth; i++ {
		levelPos = (levelPos+1)*(idx.fanOut+1) - 1
	}
	if level != idx.maxDepth {
		lastDepth := idx.size - idx.depthSizes[idx.maxDepth-1]

		newPos := (levelPos+1)*(idx.fanOut+1) - 1

		levelPos += lastDepth
		if newPos < levelPos {
			levelPos = newPos
		}
	}

	return levelPos
}

func main() {
	tr := BTree{}
	err := tr.Open("test.btree", 0666, 16, 16, 3, false)
	if err != nil {
		log.Fatal(err)
	}

	size := 16

	data := make(map[string][]byte)
	for i := 0; i < size; i++ {
		data[fmt.Sprintf("wow-%d", i)] = []byte(fmt.Sprintf("%16d", i))
	}

	rootIndex, err := tr.WriteRecords(data)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Wrote at index", rootIndex)

	for i := 0; i < size; i++ {
		data, _, err := tr.Find(rootIndex, fmt.Sprintf("wow-%d", i))
		if err != nil {
			log.Fatal(err)
		}

		if data == nil {
			log.Fatal("no data found")
		} else {
			fmt.Println("Got data:", data)
		}
	}

	offset := uint64(0)
	for {
		cnt := 0
		complete, err := tr.Scan(rootIndex, offset, func(off uint64, index IndexType, key string, val []byte) bool {
			cnt++
			if cnt > 1 {
				offset = off
				return false
			}
			fmt.Println("Found", key)
			return true
		})
		if err != nil {
			log.Fatal(err)
		}
		if complete {
			break
		}
	}
}
