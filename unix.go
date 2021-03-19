package casfs

import (
	"os"
	"syscall"
)

func S_ISDIR(mode uint32) bool {
	return ((mode & syscall.S_IFMT) == syscall.S_IFDIR)
}

func S_ISREG(mode uint32) bool {
	return ((mode & syscall.S_IFMT) == syscall.S_IFREG)
}

func S_ISBLK(mode uint32) bool {
	return ((mode & syscall.S_IFMT) == syscall.S_IFBLK)
}

func S_ISCHR(mode uint32) bool {
	return ((mode & syscall.S_IFMT) == syscall.S_IFCHR)
}

func UnixToFileStatMode(unixMode uint32) os.FileMode {
	fsMode := os.FileMode(unixMode & 0777)
	switch unixMode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		fsMode |= os.ModeDevice
	case syscall.S_IFCHR:
		fsMode |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		fsMode |= os.ModeDir
	case syscall.S_IFIFO:
		fsMode |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		fsMode |= os.ModeSymlink
	case syscall.S_IFREG:
		// nothing to do
	case syscall.S_IFSOCK:
		fsMode |= os.ModeSocket
	}
	if (unixMode & syscall.S_ISGID) != 0 {
		fsMode |= os.ModeSetgid
	}
	if (unixMode & syscall.S_ISUID) != 0 {
		fsMode |= os.ModeSetuid
	}
	if (unixMode & syscall.S_ISVTX) != 0 {
		fsMode |= os.ModeSticky
	}
	return fsMode
}


func FileStatToUnixMode(fsMode os.FileMode) uint32 {
	unixMode := uint32(fsMode & 0777)
	if (fsMode & os.ModeCharDevice) != 0 {
		unixMode |= syscall.S_IFCHR
	} else if (fsMode & os.ModeDevice) != 0 {
		unixMode |= syscall.S_IFBLK
	} else if (fsMode & os.ModeDir) != 0 {
		unixMode |= syscall.S_IFDIR
	} else if (fsMode & os.ModeNamedPipe) != 0 {
		unixMode |= syscall.S_IFIFO
	} else if (fsMode & os.ModeSymlink) != 0 {
		unixMode |= syscall.S_IFLNK
	} else if (fsMode & os.ModeSocket) != 0 {
		unixMode |= syscall.S_IFSOCK
	} else {
		unixMode |= syscall.S_IFREG
	}
	if (fsMode & os.ModeSetgid) != 0 {
		unixMode |= syscall.S_ISGID
	}
	if (fsMode & os.ModeSetuid) != 0 {
		unixMode |= syscall.S_ISUID
	}
	if (fsMode & os.ModeSticky) != 0 {
		unixMode |= syscall.S_ISVTX
	}
	return unixMode
}
