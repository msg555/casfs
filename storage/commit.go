package storage

func (mnt *MountView) Commit(tag string) error {
	return nil
	// List all nodes 
/*
	mnt.InodeMap.Enumerate(func(srcInodeId InodeId, dstInodeId InodeId) {
	})
*/


	/*
	Recursively commit all content within this mount into main storage.

	inodes representing identical data should alias with any existing identical
	inode and not create a new entry. If multiple inodes have identical data they
	should increment a "hardlink" counter in the second and ultimately create a
	new inode in main storage (although their block data should still alias, only
	metadata will be duplicated in this case).

	File data should have zero'ed holes punched out prior to insertion into stable
	storage.

	To support aliasing the b-tree structure should be canonicalized, i.e. it
	should be transformed into a path-invariant form that is a function only of
	the content.

	Every block can be divided up into one of several types. All blocks other than
	tag b-tree blocks are ref-counted and will be deleted when they are no longer
  reachable from the tag b-tree.
	
	* simple inode block
		-> has no further refs, e.g. a named pipe
	* file inode block
    -> references a file b-tree block or data blocks
  * dir inode block
		-> references a dir b-tree block or other inode blocks
  * file b-tree block
    -> references other file b-tree blocks and/or data blocks
  * dir b-tree block
    -> references other dir b-tree blocks and inode blocks (type can be inferred)
  * tag b-tree block
    -> references other tag b-tree blocks and dir inode blocks

	Perhaps the "tag btree" should be stored in bolt or similar

	Perhaps ref counting should also happen in bolt

	Need content addressed index
	*/
	return nil
}
