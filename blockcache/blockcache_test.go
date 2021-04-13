package blockcache

import (
	"encoding/binary"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var bo = binary.LittleEndian

// Ensure that multiple keys can be accessed at the same time.
func TestParallelismManyKey(t *testing.T) {
	numObjs := 10
	cache := New(numObjs, 4)

	wg := sync.WaitGroup{}
	wg.Add(numObjs)
	for i := 0; i < numObjs; i++ {
		go func(obj int) {
			err := cache.Access(nil, obj, true, func(_ interface{}, buf []byte, found bool) (interface{}, bool, error) {
				if found {
					t.Fatal("expected element to be created")
				}
				wg.Done()
				wg.Wait()
				return nil, false, nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}

	c := make(chan struct{})
	go func() {
		wg.Wait()
		c <- struct{}{}
	}()

	select {
	case <-c:
	case <-time.After(1000000000):
		t.Fatal("timed out waiting for routines to finish")
	}
}

// Ensure that a single key can only be accessed once at the same time. Also
// verifies that modifications to the buffer are made accessible to subsequent
// accesses.
func TestParallelismSingleKey(t *testing.T) {
	numGoros := 100
	cache := New(10, 4)

	wg := sync.WaitGroup{}
	wg.Add(numGoros)
	for i := 0; i < numGoros; i++ {
		go func() {
			err := cache.Access(nil, nil, true, func(_ interface{}, buf []byte, _ bool) (interface{}, bool, error) {
				bo.PutUint32(buf, bo.Uint32(buf)+1)
				return nil, true, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			wg.Done()
		}()
	}

	c := make(chan struct{})
	go func() {
		wg.Wait()
		c <- struct{}{}
	}()

	select {
	case <-c:
	case <-time.After(1000000000):
		t.Fatal("timed out waiting for routines to finish")
	}

	err := cache.Access(nil, nil, true, func(_ interface{}, buf []byte, _ bool) (interface{}, bool, error) {
		bufVal := bo.Uint32(buf)
		if bufVal != uint32(numGoros) {
			t.Fatalf("Expected bufVal %d but got %d", numGoros, bufVal)
		}
		return nil, false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type TestMapFlusher struct {
	Backing map[int]int
}

func (f *TestMapFlusher) FlushBlock(key interface{}, tag interface{}, buf []byte) (interface{}, error) {
	f.Backing[key.(int)] = int(bo.Uint32(buf))
	return nil, nil
}

func TestFlushFuzz(t *testing.T) {
	cache := New(10, 4)

	group := &TestMapFlusher{
		Backing: make(map[int]int),
	}

	rng := rand.New(rand.NewSource(555))
	keyDomain := 20

	for i := 0; i < 10000; i++ {
		k := rng.Int() % keyDomain
		err := cache.Access(group, k, true, func(_ interface{}, buf []byte, found bool) (interface{}, bool, error) {
			if found {
				bo.PutUint32(buf, bo.Uint32(buf)+1)
			} else {
				bo.PutUint32(buf, uint32(group.Backing[k])+1)
			}
			return nil, true, nil
		})
		if (i+1)%100 == 0 {
			keyHits := 0
			valTotal := 0
			backedValTotal := 0
			for j := 0; j < keyDomain; j++ {
				err := cache.Access(group, j, false, func(_ interface{}, buf []byte, _ bool) (interface{}, bool, error) {
					if buf == nil {
						valTotal += group.Backing[j]
						backedValTotal += group.Backing[j]
					} else {
						valTotal += int(bo.Uint32(buf))
						keyHits++
					}
					return nil, true, nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if valTotal != i+1 {
				t.Fatalf("did not find expected sum of values")
			}
			if backedValTotal >= valTotal {
				t.Fatalf("did not expect all modifications to be in backing")
			}
			if keyHits != cache.Size {
				t.Fatalf("expected number of key hits to match cache size; hits=%d cacheSize=%d", keyHits, cache.Size)
			}
			if cache.Size < 1 || cache.CacheSize < cache.Size {
				t.Fatal("unexpected cache size")
			}

			valTotal = 0
			cache.FlushGroup(group)
			for j := 0; j < keyDomain; j++ {
				valTotal += group.Backing[j]
			}
			if valTotal != i+1 {
				t.Fatalf("FlushGroup did not push all values to backing")
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	err := cache.Access(group, 0, true, func(_ interface{}, buf []byte, created bool) (interface{}, bool, error) {
		bo.PutUint32(buf, uint32(0xFFFFFFFF))
		return nil, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = cache.Flush(group, 0)
	if err != nil {
		t.Fatal(err)
	}

	if group.Backing[0] != 0xFFFFFFFF {
		t.Fatal("flush of individual element did not go to backing")
	}
}
