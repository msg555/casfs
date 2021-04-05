package btree

import (
	"errors"
)

var ErrorKeyAlreadyExists = errors.New("key already exists")

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
			return 0, 0, nil, nil, ErrorKeyAlreadyExists
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
