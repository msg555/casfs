package casfs

import (
	"os"

	"golang.org/x/sys/unix"
)

func S_ISDIR(mode uint32) bool {
	return ((mode & unix.S_IFMT) == unix.S_IFDIR)
}

func S_ISREG(mode uint32) bool {
	return ((mode & unix.S_IFMT) == unix.S_IFREG)
}

func S_ISBLK(mode uint32) bool {
	return ((mode & unix.S_IFMT) == unix.S_IFBLK)
}

func S_ISCHR(mode uint32) bool {
	return ((mode & unix.S_IFMT) == unix.S_IFCHR)
}

func UnixToFileStatMode(unixMode uint32) os.FileMode {
	fsMode := os.FileMode(unixMode & 0777)
	switch unixMode & unix.S_IFMT {
	case unix.S_IFBLK:
		fsMode |= os.ModeDevice
	case unix.S_IFCHR:
		fsMode |= os.ModeDevice | os.ModeCharDevice
	case unix.S_IFDIR:
		fsMode |= os.ModeDir
	case unix.S_IFIFO:
		fsMode |= os.ModeNamedPipe
	case unix.S_IFLNK:
		fsMode |= os.ModeSymlink
	case unix.S_IFREG:
		// nothing to do
	case unix.S_IFSOCK:
		fsMode |= os.ModeSocket
	}
	if (unixMode & unix.S_ISGID) != 0 {
		fsMode |= os.ModeSetgid
	}
	if (unixMode & unix.S_ISUID) != 0 {
		fsMode |= os.ModeSetuid
	}
	if (unixMode & unix.S_ISVTX) != 0 {
		fsMode |= os.ModeSticky
	}
	return fsMode
}


func FileStatToUnixMode(fsMode os.FileMode) uint32 {
	unixMode := uint32(fsMode & 0777)
	if (fsMode & os.ModeCharDevice) != 0 {
		unixMode |= unix.S_IFCHR
	} else if (fsMode & os.ModeDevice) != 0 {
		unixMode |= unix.S_IFBLK
	} else if (fsMode & os.ModeDir) != 0 {
		unixMode |= unix.S_IFDIR
	} else if (fsMode & os.ModeNamedPipe) != 0 {
		unixMode |= unix.S_IFIFO
	} else if (fsMode & os.ModeSymlink) != 0 {
		unixMode |= unix.S_IFLNK
	} else if (fsMode & os.ModeSocket) != 0 {
		unixMode |= unix.S_IFSOCK
	} else {
		unixMode |= unix.S_IFREG
	}
	if (fsMode & os.ModeSetgid) != 0 {
		unixMode |= unix.S_ISGID
	}
	if (fsMode & os.ModeSetuid) != 0 {
		unixMode |= unix.S_ISUID
	}
	if (fsMode & os.ModeSticky) != 0 {
		unixMode |= unix.S_ISVTX
	}
	return unixMode
}
