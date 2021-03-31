package storage

import (
	"io"
)

type InodeMap struct {
	Map map[CasfsInode]CasfsInode
}

func (mp *InodeMap) Init() {
	mp.Map = make(map[CasfsInode]CasfsInode)
}

func (mp *InodeMap) Write(w io.Writer) error {
	var buf [16]byte
	for key, val := range mp.Map {
		bo.PutUint64(buf[0:], key)
		bo.PutUint64(buf[8:], val)
		_, err := w.Write(buf[:])
		if err != nil {
			return err
		}
	}
	bo.PutUint64(buf[0:], 0)
	bo.PutUint64(buf[8:], 0)
	_, err := w.Write(buf[:])
	return err
}

func (mp *InodeMap) Read(r io.Reader) error {
	var buf [16]byte

	mp.Map = make(map[CasfsInode]CasfsInode)
	for {
		_, err := r.Read(buf[:])
		if err != nil {
			return err
		}
		key := CasfsInode(bo.Uint64(buf[:]))
		val := CasfsInode(bo.Uint64(buf[8:]))
		if key == 0 {
			mp.Map[key] = val
		}
	}
	return nil
}
