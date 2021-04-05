// Implements a simple, immutable B-tree implementation on disk featuring fixed
// length keys and values. Keys are strings that have variable length not
// exceeding MaxKeyLength. Values are fixed length byte arrays of size
// RecordLength.
// Has a simple API for constructing the B-tree all at once or by writing
// it record by record in sorted order.

package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/go-errors/errors"

	"github.com/msg555/ctrfs/blockfile"
)

var bo = binary.LittleEndian

type KeyType = []byte
type ValueType = []byte
type IndexType = uint64
type TreeIndex = uint64

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
	// child nodes. FanOut must be set to an even number.
	FanOut int

	// The maximum depth this tree can be forked. In practice this will be 2 when
	// used in ctrfs.
	MaxForkDepth int

	// Number of bytes in total for an entry
	nodeSize int

	// Underlying block store to keep b-tree nodes
	blocks    []*blockfile.BlockFile
	forkDepth int
}

/*
Block:
	A macro-node in a B-tree containing up to FanOut "nodes"
Node:
	A single key/value entry stored within a block

Block Layout
	nodes  uint32
	height uint32 - leaf nodes have height=0

	... 0 <= i <= tr.FanOut
	child[i] uint64

	... 0 <= i < tr.FanOut (set only for i < nodes)
	keylen[i] uint32
	key[i]    [MaxKeySize]byte
	value[i]  [EntrySize]byte
*/

func dupBytes(arr []byte) []byte {
	result := make([]byte, len(arr))
	copy(result, arr)
	return result
}

// Open a B-tree with a single writable block allocator.
func (tr *BTree) Open(path string, perm os.FileMode) error {
	if tr.FanOut%2 == 1 {
		return errors.New("tree fan out must be even")
	}

	nodeSize := 4 + tr.MaxKeySize + tr.EntrySize

	bf := blockfile.BlockFile{
		BlockSize: 4 + 8*(tr.FanOut+1) + nodeSize*tr.FanOut,
	}
	err := bf.Open(path, perm, false)
	if err != nil {
		return err
	}

	tr.nodeSize = nodeSize
	tr.blocks = []*blockfile.BlockFile{&bf}
	tr.forkDepth = 0
	return nil
}

// Forks a B-tree to create a new tree where writes are written only to a new
// separate block file. This forked tree's lifetime must be shorter than its
// parent as it relies on resources owned and managed by the parent tree.
func (tr *BTree) ForkFrom(parent *BTree, path string, perm os.FileMode) error {
	if len(parent.blocks)+1 >= parent.MaxForkDepth {
		return errors.New("parent already at maximum fork depth")
	}

	bf := blockfile.BlockFile{
		BlockSize: parent.blocks[0].BlockSize,
	}
	err := bf.Open(path, perm, false)
	if err != nil {
		return err
	}

	tr.MaxKeySize = parent.MaxKeySize
	tr.EntrySize = parent.EntrySize
	tr.FanOut = parent.FanOut
	tr.MaxForkDepth = parent.MaxForkDepth

	tr.blocks = make([]*blockfile.BlockFile, len(parent.blocks)+1)
	copy(tr.blocks, parent.blocks)
	tr.blocks[len(parent.blocks)] = &bf

	tr.nodeSize = parent.nodeSize

	return nil
}

// Close resources directly managed by this B-tree. Note that upon closing a
// B-tree all forked child B-trees will no longer be usable and should also be
// closed.
func (tr *BTree) Close() error {
	if tr.blocks != nil {
		return tr.blocks[len(tr.blocks)-1].Close()
	}
	return nil
}

func (tr *BTree) validateKeyValue(key KeyType, value ValueType) error {
	if len(key) == 0 {
		return errors.New("key length too small")
	}
	if len(key) > tr.MaxKeySize {
		return errors.New("key length too long")
	}
	if len(value) != tr.EntrySize {
		return errors.New("value size incorrect")
	}
	return nil
}

func (tr *BTree) readBlock(index TreeIndex, buf []byte) ([]byte, error) {
	blockIndex := index / TreeIndex(tr.MaxForkDepth)
	blockForkDepth := index % TreeIndex(tr.MaxForkDepth)
	if blockForkDepth >= TreeIndex(len(tr.blocks)) {
		return nil, errors.New("invalid fork depth for current tree")
	}
	return tr.blocks[blockForkDepth].Read(blockIndex, buf)
}

func (tr *BTree) freeBlock(treeIndex TreeIndex) error {
	blockIndex := treeIndex / TreeIndex(tr.MaxForkDepth)
	blockForkDepth := treeIndex % TreeIndex(tr.MaxForkDepth)
	if blockForkDepth+1 == TreeIndex(len(tr.blocks)) {
		return tr.blocks[blockForkDepth].Free(blockIndex)
	}
	return nil
}

func (tr *BTree) newBlock(block []byte) (TreeIndex, error) {
	newIndex, err := tr.blocks[len(tr.blocks)-1].Allocate()
	if err != nil {
		return 0, err
	}
	err = tr.blocks[len(tr.blocks)-1].Write(newIndex, block)
	if err != nil {
		return 0, err
	}
	return newIndex*TreeIndex(tr.MaxForkDepth) + TreeIndex(len(tr.blocks)-1), nil
}

func (tr *BTree) copyUpBlock(index TreeIndex, block []byte) (TreeIndex, error) {
	if tr.getBlockSize(block) == 0 {
		return 0, errors.New("cannot save block with size 0")
	}
	blockForkDepth := index % TreeIndex(tr.MaxForkDepth)
	if blockForkDepth+1 == TreeIndex(len(tr.blocks)) {
		blockIndex := index / TreeIndex(tr.MaxForkDepth)
		if err := tr.blocks[blockForkDepth].Write(blockIndex, block); err != nil {
			return 0, err
		}
		return index, nil
	}
	return tr.newBlock(block)
}

func (tr *BTree) getBlockSize(block []byte) int {
	return int(bo.Uint32(block))
}

func (tr *BTree) setBlockSize(block []byte, size int) {
	bo.PutUint32(block, uint32(size))
}

func (tr *BTree) getBlockChild(block []byte, childInd int) TreeIndex {
	if childInd < 0 || childInd > tr.FanOut {
		panic("child index out of range")
	}
	return bo.Uint64(block[4+8*childInd:])
}

func (tr *BTree) setBlockChild(block []byte, childInd int, childTr TreeIndex) {
	if childInd < 0 || childInd > tr.FanOut {
		panic("child index out of range")
	}
	bo.PutUint64(block[4+8*childInd:], childTr)
}

func (tr *BTree) getNodeSlice(block []byte, i int) []byte {
	posStart := 4 + 8*(tr.FanOut+1) + i*tr.nodeSize
	return block[posStart : posStart+tr.nodeSize]
}

func (tr *BTree) getNodeKey(node []byte) KeyType {
	keylen := int(bo.Uint32(node))
	if keylen > tr.MaxKeySize {
		return nil
	}
	return node[4 : 4+keylen]
}

func (tr *BTree) getNodeValue(node []byte) ValueType {
	return node[4+tr.MaxKeySize : tr.nodeSize]
}

func (tr *BTree) setNode(node []byte, key KeyType, value ValueType) {
	bo.PutUint32(node, uint32(len(key)))
	copy(node[4:], key)
	copy(node[4+tr.MaxKeySize:], value)
}

func (tr *BTree) searchBlock(block []byte, key KeyType) (int, bool, error) {
	lo := 0
	hi := tr.getBlockSize(block) - 1
	if hi < 0 || hi >= tr.FanOut {
		return 0, false, errors.New("unexpected block size")
	}
	for {
		md := lo + (hi-lo)/2
		nodeData := tr.getNodeSlice(block, md)

		nodeKey := tr.getNodeKey(nodeData)
		if len(key) == 0 {
			return 0, false, errors.New("unexpected key")
		}

		cmp := bytes.Compare(key, nodeKey)
		if cmp == 0 {
			return md, true, nil
		} else if cmp == -1 {
			hi = md - 1
		} else {
			lo = md + 1
		}
		if hi < lo {
			if cmp > 0 {
				md++
			}
			return md, false, nil
		}
	}
}

func (tr *BTree) Insert(treeIndex TreeIndex, key KeyType, value ValueType, overwrite bool) (TreeIndex, error) {
	err := tr.validateKeyValue(key, value)
	if err != nil {
		return 0, err
	}

	var tr1 TreeIndex
	var tr2 TreeIndex

	if treeIndex != 0 {
		tr1, tr2, key, value, err = tr.insertHelper(treeIndex, key, value, overwrite)
		if err != nil {
			return 0, err
		}
		if tr2 == 0 {
			return tr1, nil
		}
	}

	// Need to create new root node.
	block := make([]byte, tr.blocks[0].BlockSize)
	tr.setBlockSize(block, 1)
	tr.setBlockChild(block, 0, tr1)
	tr.setBlockChild(block, 1, tr2)
	tr.setNode(tr.getNodeSlice(block, 0), key, value)

	treeIndex, err = tr.newBlock(block)
	if err != nil {
		return 0, err
	}
	return treeIndex, nil
}

func (tr *BTree) insertHelper(treeIndex TreeIndex, key KeyType, value ValueType, overwrite bool) (TreeIndex, TreeIndex, KeyType, ValueType, error) {
	block, err := tr.readBlock(treeIndex, nil)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	insertInd, match, err := tr.searchBlock(block, key)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	// Handle case where there's an exact key match.
	if match {
		if !overwrite {
			return 0, 0, nil, nil, errors.New("entry already exists")
		}

		tr.setNode(tr.getNodeSlice(block, insertInd), key, value)
		treeIndex, err = tr.copyUpBlock(treeIndex, block)
		if err != nil {
			return 0, 0, nil, nil, err
		}

		return treeIndex, 0, nil, nil, nil
	}

	var tr1 TreeIndex
	var tr2 TreeIndex

	// Continue inserting down the tree if needed.
	childTree := tr.getBlockChild(block, insertInd)
	if childTree != 0 {
		tr1, tr2, key, value, err = tr.insertHelper(childTree, key, value, overwrite)
		if err != nil {
			return 0, 0, nil, nil, err
		}

		if tr2 == 0 {
			// Insert into child was clean
			tr.setBlockChild(block, insertInd, tr1)

			treeIndex, err = tr.copyUpBlock(treeIndex, block)
			if err != nil {
				return 0, 0, nil, nil, err
			}
			return treeIndex, 0, nil, nil, err
		}
	}

	// We have to insert new node (key, value) with children (tr1, tr2)
	blockSize := tr.getBlockSize(block)
	if blockSize < tr.FanOut {
		// We have room for the new child directly in our block.

		// Shift over our child pointers
		for i := blockSize; i >= insertInd+1; i-- {
			tr.setBlockChild(block, i+1, tr.getBlockChild(block, i))
		}
		tr.setBlockChild(block, insertInd+1, tr2)
		tr.setBlockChild(block, insertInd, tr1)

		// Shift over our node data
		for i := blockSize - 1; i >= insertInd; i-- {
			copy(tr.getNodeSlice(block, i+1), tr.getNodeSlice(block, i))
		}
		tr.setNode(tr.getNodeSlice(block, insertInd), key, value)

		tr.setBlockSize(block, blockSize+1)

		treeIndex, err = tr.copyUpBlock(treeIndex, block)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		return treeIndex, 0, nil, nil, nil
	}

	// List out the children and nodes in order
	var childTrees []TreeIndex
	var nodeKeys []KeyType
	var nodeValues []ValueType
	for i := 0; i <= blockSize; i++ {
		if i == insertInd {
			childTrees = append(childTrees, tr1)
			childTrees = append(childTrees, tr2)
			nodeKeys = append(nodeKeys, key)
			nodeValues = append(nodeValues, value)
		} else {
			childTrees = append(childTrees, tr.getBlockChild(block, i))
		}
		if i < blockSize {
			nodeKeys = append(nodeKeys, tr.getNodeKey(tr.getNodeSlice(block, i)))
			nodeValues = append(nodeValues, tr.getNodeValue(tr.getNodeSlice(block, i)))
		}
	}

	blockA := make([]byte, tr.blocks[0].BlockSize)
	blockB := make([]byte, tr.blocks[0].BlockSize)

	newBlockSize := tr.FanOut / 2
	tr.setBlockSize(blockA, newBlockSize)
	tr.setBlockSize(blockB, newBlockSize)

	for i := 0; i <= newBlockSize; i++ {
		tr.setBlockChild(blockA, i, childTrees[i])
		tr.setBlockChild(blockB, i, childTrees[newBlockSize+i+1])
		if i < newBlockSize {
			tr.setNode(tr.getNodeSlice(blockA, i), nodeKeys[i], nodeValues[i])
			tr.setNode(tr.getNodeSlice(blockB, i), nodeKeys[newBlockSize+i+1], nodeValues[newBlockSize+i+1])
		}
	}

	// Otherwise we will have to split our own node as well
	treeIndexA, err := tr.copyUpBlock(treeIndex, blockA)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	treeIndexB, err := tr.newBlock(blockB)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return treeIndexA, treeIndexB, nodeKeys[newBlockSize], nodeValues[newBlockSize], nil
}

func (tr *BTree) WriteRecords(data map[string]ValueType) (TreeIndex, error) {
	treeNode := TreeIndex(0)
	for key, value := range data {
		var err error
		treeNode, err = tr.Insert(treeNode, KeyType(key), value, false)
		if err != nil {
			return 0, err
		}
	}
	return treeNode, nil
}

func (tr *BTree) Delete(treeIndex TreeIndex, key KeyType) (TreeIndex, error) {
	if key == nil {
		return 0, errors.New("key cannot be nil")
	}

	block, _, _, err := tr.deleteHelper(treeIndex, key)
	if err != nil {
		return 0, err
	}

	if tr.getBlockSize(block) == 0 {
		// Root has emptied out, delete root node
		if err := tr.freeBlock(treeIndex); err != nil {
			return 0, err
		}
		return tr.getBlockChild(block, 0), nil
	}

	return tr.copyUpBlock(treeIndex, block)
}

func (tr *BTree) deleteHelper(treeIndex TreeIndex, key KeyType) ([]byte, KeyType, ValueType, error) {
	// TODO: probably can avoid some copyUp invocations if nothing changed
	block, err := tr.readBlock(treeIndex, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	var insertInd int
	var match bool
	if key == nil {
		// nil key means we want to delete max element
		insertInd = tr.getBlockSize(block)
		if tr.getBlockChild(block, insertInd) == 0 {
			insertInd--
			match = true
		}
	} else {
		insertInd, match, err = tr.searchBlock(block, key)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	childTree := tr.getBlockChild(block, insertInd)
	var childBlock []byte
	var deletedKey KeyType
	var deletedValue ValueType

	blockSize := tr.getBlockSize(block)
	if match {
		if childTree == 0 {
			if key == nil {
				// we only need to copy the deleted key/value if we were doing a deleted
				// max operation.
				deletedKey = dupBytes(tr.getNodeKey(tr.getNodeSlice(block, insertInd)))
				deletedValue = dupBytes(tr.getNodeValue(tr.getNodeSlice(block, insertInd)))
			}

			// Shift over elements on top of deleted element, no children to shift.
			for i := insertInd; i+1 < blockSize; i++ {
				copy(tr.getNodeSlice(block, i), tr.getNodeSlice(block, i+1))
			}
			tr.setBlockSize(block, blockSize-1)

			return block, deletedKey, deletedValue, nil
		}

		childBlock, deletedKey, deletedValue, err = tr.deleteHelper(childTree, nil)
		if err != nil {
			return nil, nil, nil, err
		}

		tr.setNode(tr.getNodeSlice(block, insertInd), deletedKey, deletedValue)
	} else {
		childBlock, deletedKey, deletedValue, err = tr.deleteHelper(childTree, key)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	childBlockSize := tr.getBlockSize(childBlock)
	if childBlockSize*2 >= tr.FanOut {
		// Child size is fine
		childTree, err := tr.copyUpBlock(childTree, childBlock)
		if err != nil {
			return nil, nil, nil, err
		}

		tr.setBlockChild(block, insertInd, childTree)
		return block, deletedKey, deletedValue, nil
	}

	// Child is too small, match up with sibling
	sibIndex := insertInd - 1
	if sibIndex < 0 {
		sibIndex = insertInd + 1
	}
	sibTree := tr.getBlockChild(block, sibIndex)
	sibBlock, err := tr.readBlock(sibTree, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	sibBlockSize := tr.getBlockSize(sibBlock)

	// Normalize so that childX refers to the larger sibling
	if insertInd < sibIndex {
		sibIndex, insertInd = insertInd, sibIndex
		sibBlock, childBlock = childBlock, sibBlock
		sibTree, childTree = childTree, sibTree
		sibBlockSize, childBlockSize = childBlockSize, sibBlockSize
	}

	if childBlockSize+sibBlockSize >= tr.FanOut {
		// Rebalance nodes with sibling block
		var childTrees []TreeIndex
		var nodeKeys []KeyType
		var nodeValues []ValueType
		for i := 0; i <= sibBlockSize; i++ {
			childTrees = append(childTrees, tr.getBlockChild(sibBlock, i))
			if i < sibBlockSize {
				nodeKeys = append(nodeKeys, tr.getNodeKey(tr.getNodeSlice(sibBlock, i)))
				nodeValues = append(nodeValues, tr.getNodeValue(tr.getNodeSlice(sibBlock, i)))
			}
		}
		nodeKeys = append(nodeKeys, tr.getNodeKey(tr.getNodeSlice(block, sibIndex)))
		nodeValues = append(nodeValues, tr.getNodeValue(tr.getNodeSlice(block, sibIndex)))
		for i := 0; i <= childBlockSize; i++ {
			childTrees = append(childTrees, tr.getBlockChild(childBlock, i))
			if i < childBlockSize {
				nodeKeys = append(nodeKeys, tr.getNodeKey(tr.getNodeSlice(childBlock, i)))
				nodeValues = append(nodeValues, tr.getNodeValue(tr.getNodeSlice(childBlock, i)))
			}
		}

		// Calculate new block sizes
		sibBlockSize = (len(nodeKeys) - 1) / 2
		childBlockSize = len(nodeKeys) - 1 - sibBlockSize

		// Copy values back into blocks
		sibBlock = make([]byte, tr.blocks[0].BlockSize)
		tr.setBlockSize(sibBlock, sibBlockSize)
		for i := 0; i <= sibBlockSize; i++ {
			tr.setBlockChild(sibBlock, i, childTrees[i])
			if i < sibBlockSize {
				tr.setNode(tr.getNodeSlice(sibBlock, i), nodeKeys[i], nodeValues[i])
			}
		}

		childBlock = make([]byte, tr.blocks[0].BlockSize)
		tr.setBlockSize(childBlock, childBlockSize)
		for i := 0; i <= childBlockSize; i++ {
			tr.setBlockChild(childBlock, i, childTrees[sibBlockSize+1+i])
			if i < childBlockSize {
				tr.setNode(tr.getNodeSlice(childBlock, i), nodeKeys[sibBlockSize+1+i], nodeValues[sibBlockSize+1+i])
			}
		}

		sibTree, err = tr.copyUpBlock(sibTree, sibBlock)
		if err != nil {
			return nil, nil, nil, err
		}

		childTree, err = tr.copyUpBlock(childTree, childBlock)
		if err != nil {
			return nil, nil, nil, err
		}

		tr.setNode(tr.getNodeSlice(block, sibIndex), nodeKeys[sibBlockSize], nodeValues[sibBlockSize])
		tr.setBlockChild(block, sibIndex, sibTree)
		tr.setBlockChild(block, sibIndex+1, childTree)

		return block, deletedKey, deletedValue, nil
	}

	// Merge with sibling block
	copy(tr.getNodeSlice(sibBlock, sibBlockSize), tr.getNodeSlice(block, sibIndex))
	for i := 0; i <= childBlockSize; i++ {
		tr.setBlockChild(sibBlock, sibBlockSize+1+i, tr.getBlockChild(childBlock, i))
		if i < childBlockSize {
			copy(tr.getNodeSlice(sibBlock, sibBlockSize+1+i), tr.getNodeSlice(childBlock, i))
		}
	}
	tr.setBlockSize(sibBlock, sibBlockSize+childBlockSize+1)

	sibTree, err = tr.copyUpBlock(sibTree, sibBlock)
	if err != nil {
		return nil, nil, nil, err
	}

	for i := sibIndex; i+1 < blockSize; i++ {
		tr.setBlockChild(block, i+1, tr.getBlockChild(block, i+2))
		copy(tr.getNodeSlice(block, i), tr.getNodeSlice(block, i+1))
	}
	tr.setBlockSize(block, blockSize-1)

	return block, deletedKey, deletedValue, nil
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
func (tr *BTree) Scan(treeIndex TreeIndex, offset uint64, entryCallback func(offset uint64, index IndexType, key KeyType, value ValueType) bool) (bool, error) {
	if treeIndex == 0 {
		return true, nil
	}

	var stackBlocks [][]byte
	var stackBlockIndexes []TreeIndex
	var stackIndexes []int
	for {
		block, err := tr.readBlock(treeIndex, nil)
		if err != nil {
			return false, err
		}

		childIndex := offset % uint64(tr.FanOut+1)
		stackBlocks = append(stackBlocks, block)
		stackBlockIndexes = append(stackBlockIndexes, treeIndex)
		stackIndexes = append(stackIndexes, int(childIndex))
		if offset <= uint64(tr.FanOut) {
			break
		}

		offset /= uint64(tr.FanOut + 1)
		treeIndex = tr.getBlockChild(block, int(childIndex))
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
				childIndex := tr.getBlockChild(stackBlocks[stackDepth], stackIndexes[stackDepth])
				if childIndex == 0 {
					break
				}

				block, err := tr.readBlock(childIndex, nil)
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
			if index == tr.getBlockSize(block) {
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
		nodeData := tr.getNodeSlice(block, index)

		key := tr.getNodeKey(nodeData)
		if len(key) == 0 {
			return false, errors.New("unexpected key")
		}

		value := tr.getNodeValue(nodeData)

		offset = uint64(index + 1)
		for i := stackDepth - 1; i >= 0; i-- {
			offset = offset*uint64(tr.FanOut+1) + uint64(stackIndexes[i])
		}
		if !entryCallback(offset, blockIndex*uint64(tr.FanOut)+uint64(index), key, value) {
			return false, nil
		}

		stackIndexes[stackDepth]++
	}
}

func (tr *BTree) Find(treeIndex TreeIndex, key KeyType) (ValueType, IndexType, error) {
	for {
		for treeIndex == 0 {
			return nil, 0, nil
		}

		block, err := tr.readBlock(treeIndex, nil)
		if err != nil {
			return nil, 0, err
		}

		insertInd, match, err := tr.searchBlock(block, key)
		if err != nil {
			return nil, 0, err
		}

		if match {
			return tr.getNodeValue(tr.getNodeSlice(block, insertInd)), treeIndex*uint64(tr.FanOut) + uint64(insertInd), nil
		}

		treeIndex = tr.getBlockChild(block, insertInd)
	}
}

func (tr *BTree) ByIndex(index IndexType) (ValueType, error) {
	treeIndex := TreeIndex(index / uint64(tr.FanOut))
	pos := int(index % uint64(tr.FanOut))

	block, err := tr.readBlock(treeIndex, nil)
	if err != nil {
		return nil, err
	}
	return tr.getNodeValue(tr.getNodeSlice(block, pos)), nil
}

func main() {
	tr := BTree{
		MaxKeySize:   16,
		EntrySize:    16,
		FanOut:       4,
		MaxForkDepth: 2,
	}
	err := tr.Open("test.btree", 0666)
	if err != nil {
		log.Fatal(err)
	}

	insertSize := 3200

	rootIndex := TreeIndex(0)
	for i := 0; i < insertSize; i++ {
		rootIndex, err = tr.Insert(rootIndex, []byte(fmt.Sprintf("wow-%d", i)), []byte(fmt.Sprintf("%16d", i)), false)
		if err != nil {
			log.Fatal(err)
		}
	}

	for i := 0; i < insertSize/3; i++ {
		key := []byte(fmt.Sprintf("wow-%d", i))
		rootIndex, err = tr.Delete(rootIndex, key)
		if err != nil {
			gerr, ok := err.(*errors.Error)
			if ok {
				log.Fatalf("failed: %s\n%s", err, gerr.ErrorStack())
			} else {
				log.Fatalf("failed: %s", err)
			}
		}
	}

	for i := 0; i < insertSize; i++ {
		key := []byte(fmt.Sprintf("wow-%d", i))
		data, _, err := tr.Find(rootIndex, key)
		if err != nil {
			gerr, ok := err.(*errors.Error)
			if ok {
				log.Fatalf("failed: %s\n%s", err, gerr.ErrorStack())
			} else {
				log.Fatalf("failed: %s", err)
			}
		}

		if data == nil {
			log.Printf("NOT FOUND '%s'\n", string(key))
			if i >= insertSize/3 {
				panic("expected to find")
			}
		}
	}

	offset := uint64(0)
	for {
		cnt := 0
		complete, err := tr.Scan(rootIndex, offset, func(off uint64, index IndexType, key []byte, val []byte) bool {
			cnt++
			if cnt > 1 {
				offset = off
				return false
			}
			fmt.Println(string(key), string(val))
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
