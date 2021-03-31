package fusefs

import (
	"github.com/msg555/casfs"
	"github.com/msg555/casfs/storage"
	// "github.com/msg555/casfs/unix"
)

const DIRENT_OFFSET_EOF = 0xFFFFFFFFFFFFFFFF

/*
TODOLook at fill_dir at https://github.com/libfuse/libfuse/blob/master/lib/fuse.c
to see how to add entries to the directory listing buffer.

Also see
https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details
for further notes about how fill_dir is meant to be used.

https://libfuse.github.io/doxygen/fuse__lowlevel_8h.html#ad1957bcc8ece8c90f16c42c4daf3053f
*/
func direntAlign(x int) int {
	return (x + 7) &^ 7
}

func updateDirEntryOffset(buf []byte, offset uint64) {
	casfs.Hbo.PutUint64(buf[8:], offset)
}

func addDirEntry(buf []byte, name string, ent *storage.Dirent) int {
	/*
	   define FUSE_DIRENT_ALIGN(x) (((x) + sizeof(__u64) - 1) & ~(sizeof(__u64) - 1))

	   struct fuse_dirent {
	     u64   ino;
	     u64   off;
	     u32   namelen;
	     u32   type;
	     char name[];
	   };
	*/

	entryBaseLen := 24 + len(name)
	entryPadLen := direntAlign(entryBaseLen)
	if len(buf) < entryPadLen {
		return 0
	}

	casfs.Hbo.PutUint64(buf[0:], ent.Inode)
	casfs.Hbo.PutUint32(buf[16:], uint32(len(name)))
	casfs.Hbo.PutUint32(buf[20:], uint32(ent.Type))

	copy(buf[24:], name)
	for i := entryBaseLen; i < entryPadLen; i++ {
		buf[i] = 0
	}

	return entryPadLen
}
