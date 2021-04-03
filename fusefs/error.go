package fusefs

import (
	"os"

	"bazil.org/fuse"
	"github.com/msg555/casfs/unix"
)

type FuseError struct {
	source error
	errno  unix.Errno
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
		case FuseError:
			return err.(FuseError)
		case *os.PathError:
			e = e.(*os.PathError).Err
		case unix.Errno:
			return FuseError{
				source: err,
				errno:  e.(unix.Errno),
			}
		default:
			return FuseError{
				source: err,
				errno:  unix.EIO,
			}
		}
	}
}
