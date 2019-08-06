package kademlia

import (
	"math/big"
	"sync"
	"time"
)

type kBucket struct {
	// 0 for head, most-recently seen at the tail
	size         int
	arr          [bucketSize]Contact
	mutex        sync.Mutex
	latestUpdate time.Time
}

func (o *kBucket) update(t Contact) {
	t.Id = new(big.Int).Set(t.Id)
	o.mutex.Lock()
	defer func() {
		o.mutex.Unlock()
		o.latestUpdate = time.Now()
	}()

	for i := 0; i < o.size; i++ {
		if Ping(o.arr[i].Ip) == false {
			for j := i; j < o.size-1; j++ {
				o.arr[j] = o.arr[j+1]
			}
			o.size--
		}
	}

	for i := 0; i < o.size; i++ {
		if o.arr[i].Ip == t.Ip {
			for j := i; j < o.size-1; j++ {
				o.arr[j] = o.arr[j+1]
			}
			o.arr[o.size-1] = t
			return
		}
	}
	if o.size < bucketSize {
		o.arr[o.size] = t
		o.size++
		return
	}
	success := Ping(o.arr[0].Ip)
	if success == true {
		tmp := o.arr[0]
		for i := 0; i < o.size-1; i++ {
			o.arr[i] = o.arr[i+1]
		}
		o.arr[o.size-1] = tmp
	} else {
		for i := 0; i < o.size-1; i++ {
			o.arr[i] = o.arr[i+1]
		}
		o.arr[o.size-1] = t
	}
}
