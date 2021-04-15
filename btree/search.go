package btree

import (
	"bytes"

	"github.com/go-errors/errors"
)

func (tr *BTree) searchBlock(block []byte, key KeyType) (int, bool, error) {
	if key == nil {
		return 0, false, nil
	}

	lo := 0
	hi := tr.getBlockSize(block) - 1
	if hi < 0 {
		return 0, false, nil
	}
	if hi >= tr.FanOut {
		return 0, false, errors.New("unexpected block size")
	}
	for {
		md := lo + (hi-lo)/2
		nodeData := tr.getNodeSlice(block, md)

		nodeKey := tr.getNodeKey(nodeData)
		if len(nodeKey) == 0 {
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
func (tr *BTree) Scan(treeIndex TreeIndex, startKey KeyType, entryCallback func(index IndexType, key KeyType, value ValueType) bool) (bool, error) {
	cache := tr.blocks.GetCache()

	var stackBlocks [][]byte
	var stackBlockIndexes []TreeIndex
	var stackIndexes []int
	for treeIndex != 0 {
		block := cache.Pool.Get().([]byte)
		defer cache.Pool.Put(block)

		_, err := tr.blocks.Read(treeIndex, block)
		if err != nil {
			return false, err
		}

		insertInd, match, err := tr.searchBlock(block, startKey)
		if err != nil {
			return false, err
		}

		stackBlocks = append(stackBlocks, block)
		stackBlockIndexes = append(stackBlockIndexes, treeIndex)
		stackIndexes = append(stackIndexes, insertInd)
		if match {
			break
		}

		treeIndex = tr.getBlockChild(block, insertInd)
	}

	moveUp := false
	stackDepth := len(stackIndexes) - 1
	for {
		if moveUp {
			for {
				childIndex := tr.getBlockChild(stackBlocks[stackDepth], stackIndexes[stackDepth])
				if childIndex == 0 {
					break
				}

				var block []byte
				if stackDepth+1 < len(stackBlocks) {
					block = stackBlocks[stackDepth+1]
				} else {
					block = cache.Pool.Get().([]byte)
					defer cache.Pool.Put(block)
					stackBlocks = append(stackBlocks, block)
				}

				_, err := tr.blocks.Read(childIndex, block)
				if err != nil {
					return false, err
				}

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
		if !entryCallback(blockIndex*int64(tr.FanOut)+int64(index), key, value) {
			return false, nil
		}

		stackIndexes[stackDepth]++
	}
}

func (tr *BTree) LowerBound(treeIndex TreeIndex, key KeyType) (KeyType, ValueType, IndexType, error) {
	cache := tr.blocks.GetCache()
	block := cache.Pool.Get().([]byte)
	defer cache.Pool.Put(block)

	var resultInd IndexType
	for {
		if treeIndex == 0 {
			break
		}

		_, err := tr.blocks.Read(treeIndex, block)
		if err != nil {
			return nil, nil, 0, err
		}

		insertInd, match, err := tr.searchBlock(block, key)
		if err != nil {
			return nil, nil, 0, err
		}

		if insertInd < tr.getBlockSize(block) {
			resultInd = treeIndex*int64(tr.FanOut) + int64(insertInd)
		}
		if match {
			break
		}

		treeIndex = tr.getBlockChild(block, insertInd)
	}
	if resultInd == 0 {
		return nil, nil, 0, nil
	}

	key, val, err := tr.ByIndex(resultInd)
	return key, val, resultInd, err
}

func (tr *BTree) Find(treeIndex TreeIndex, key KeyType) (ValueType, IndexType, error) {
	lkey, lval, lind, err := tr.LowerBound(treeIndex, key)
	if err != nil {
		return nil, 0, err
	}
	if bytes.Compare(key, lkey) == 0 {
		return lval, lind, err
	}
	return nil, 0, nil
}

func (tr *BTree) ByIndex(index IndexType) (KeyType, ValueType, error) {
	cache := tr.blocks.GetCache()
	block := cache.Pool.Get().([]byte)
	defer cache.Pool.Put(block)

	treeIndex := TreeIndex(index / int64(tr.FanOut))
	pos := int(index % int64(tr.FanOut))

	_, err := tr.blocks.Read(treeIndex, block)
	if err != nil {
		return nil, nil, err
	}
	sl := tr.getNodeSlice(block, pos)
	return dupBytes(tr.getNodeKey(sl)), dupBytes(tr.getNodeValue(sl)), nil
}
