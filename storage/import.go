package storage

import (
	"github.com/msg555/ctrfs/unix"
)

type importNodeLocation struct {
	Path      string
	InodeId
}

func validatePathName(name string) bool {
	if len(name) > unix.NAME_MAX {
		return false
	}
	if name == "" || name == "." || name == ".." {
		return false
	}
	for _, ch := range name {
		if ch == 0 || ch == '/' {
			return false
		}
	}
	return true
}
