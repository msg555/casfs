package btree

import (
	"bytes"

	"github.com/go-errors/errors"
)

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
		if !entryCallback(offset, blockIndex*int64(tr.FanOut)+int64(index), key, value) {
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
			return tr.getNodeValue(tr.getNodeSlice(block, insertInd)), treeIndex*int64(tr.FanOut) + int64(insertInd), nil
		}

		treeIndex = tr.getBlockChild(block, insertInd)
	}
}

func (tr *BTree) ByIndex(index IndexType) (ValueType, error) {
	treeIndex := TreeIndex(index / int64(tr.FanOut))
	pos := int(index % int64(tr.FanOut))

	block, err := tr.readBlock(treeIndex, nil)
	if err != nil {
		return nil, err
	}
	return tr.getNodeValue(tr.getNodeSlice(block, pos)), nil
}
