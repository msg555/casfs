package main

import (
	"os"
	"syscall"

	"bazil.org/fuse"
)

type FuseError struct {
	source error
	errno  syscall.Errno
}

func (err FuseError) Error() string {
	return err.source.Error()
}

func (err FuseError) Errno() fuse.Errno {
	return fuse.Errno(err.errno)
}

func WrapIOError(err error) FuseError {
	e := err
	for {
		switch e.(type) {
		case *os.PathError:
			e = e.(*os.PathError).Err
		case syscall.Errno:
			return FuseError{
				source: err,
				errno:  e.(syscall.Errno),
			}
		default:
			return FuseError{
				source: err,
				errno:  syscall.EIO,
			}
		}
	}
}
