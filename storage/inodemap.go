package storage

import (
	"io"
)

type InodeMap struct {
	Map map[InodeId]InodeId
}

func (mp *InodeMap) Init() {
	mp.Map = make(map[InodeId]InodeId)
}

func (mp *InodeMap) Write(w io.Writer) error {
	var buf [16]byte
	for key, val := range mp.Map {
		bo.PutUint64(buf[0:], uint64(key))
		bo.PutUint64(buf[8:], uint64(val))
		_, err := w.Write(buf[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func (mp *InodeMap) Read(r io.Reader) error {
	var buf [16]byte

	mp.Map = make(map[InodeId]InodeId)
	for {
		_, err := r.Read(buf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		key := InodeId(bo.Uint64(buf[:]))
		val := InodeId(bo.Uint64(buf[8:]))
		mp.Map[key] = val
	}
	return nil
}
