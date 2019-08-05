package kademlia

import (
	"math/big"
	"sort"
	"time"
)

func (o *Node) RPCPing(p Contact, res *PingReturn) error {
	go o.O.updateBucket(p)
	*res = PingReturn{Contact{o.O.ID, o.O.IP}, true}
	return nil
}

func (o *Node) RPCStore(obj StoreRequest, res *StoreReturn) error {
	go o.O.updateBucket(obj.Header)
	o.O.Data.lock.Lock()
	if o.O.Data.Map[obj.Pair.Key] == nil {
		o.O.Data.Map[obj.Pair.Key] = make(map[string]time.Time)
	}
	o.O.Data.Map[obj.Pair.Key][obj.Pair.Val] = obj.Expire
	o.O.Data.lock.Unlock()
	*res = StoreReturn{Contact{o.O.ID, o.O.IP}, true}
	return nil
}

func (o *Node) RPCFindNode(arg FindNodeRequest, res *FindNodeReturn) error {
	go o.O.updateBucket(arg.Header)
	p := new(big.Int).Xor(arg.Id, o.O.ID).BitLen() - 1
	o.O.kBuckets[p].mutex.Lock()
	if o.O.kBuckets[p].size == bucketSize {
		*res = FindNodeReturn{Contact{o.O.ID, o.O.IP}, o.O.kBuckets[p].arr[:]}
		o.O.kBuckets[p].mutex.Unlock()
		return nil
	}
	o.O.kBuckets[p].mutex.Unlock()
	var arr []Contact
	for i := 0; i < B; i++ {
		o.O.kBuckets[i].mutex.Lock()
		for j := 0; j < o.O.kBuckets[i].size; j++ {
			arr = append(arr, o.O.kBuckets[i].arr[j])
		}
		o.O.kBuckets[i].mutex.Unlock()
	}
	sort.Slice(arr, func(i, j int) bool {
		return arr[i].Id.Cmp(arr[j].Id) < 0
	})
	length := len(arr)
	if length >= bucketSize {
		*res = FindNodeReturn{Contact{o.O.ID, o.O.IP}, arr[:20]}
	} else {
		*res = FindNodeReturn{Contact{o.O.ID, o.O.IP}, arr[:]}
	}
	return nil
}

func (o *Node) RPCFindValue(arg FindValueRequest, res *FindValueReturn) error {
	go o.O.updateBucket(arg.Header)

	value, ok := o.O.getValue(arg.Key)
	if ok {
		*res = FindValueReturn{
			Header:  Contact{o.O.ID, o.O.IP},
			Closest: nil,
			Val:     value,
		}
		return nil
	}

	p := new(big.Int).Xor(arg.HashId, o.O.ID).BitLen() - 1
	o.O.kBuckets[p].mutex.Lock()
	if o.O.kBuckets[p].size == bucketSize {
		*res = FindValueReturn{
			Header:  Contact{o.O.ID, o.O.IP},
			Closest: o.O.kBuckets[p].arr[:],
			Val:     nil,
		}
		o.O.kBuckets[p].mutex.Unlock()
		return nil
	}
	o.O.kBuckets[p].mutex.Unlock()
	var arr []Contact
	for i := 0; i < B; i++ {
		o.O.kBuckets[i].mutex.Lock()
		for j := 0; j < o.O.kBuckets[i].size; j++ {
			arr = append(arr, o.O.kBuckets[i].arr[j])
		}
		o.O.kBuckets[i].mutex.Unlock()
	}
	sort.Slice(arr, func(i, j int) bool {
		return arr[i].Id.Cmp(arr[j].Id) < 0
	})
	length := len(arr)
	if length >= bucketSize {
		*res = FindValueReturn{
			Header:  Contact{o.O.ID, o.O.IP},
			Closest: arr[:20],
			Val:     nil,
		}
	} else {
		*res = FindValueReturn{
			Header:  Contact{o.O.ID, o.O.IP},
			Closest: arr[:],
			Val:     nil,
		}
	}
	return nil
}
