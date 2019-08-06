package kademlia

import (
	"math/big"
	"sort"
	"time"
)

func (o *Node) RPCPing(p Contact, res *PingReturn) error {
	go o.O.updateBucket(p)
	*res = PingReturn{Contact{new(big.Int).Set(o.O.ID), o.O.IP}, true}
	return nil
}

func (o *Node) RPCStore(obj StoreRequest, res *StoreReturn) error {
	go o.O.updateBucket(obj.Header)
	o.O.Data.lock.Lock()
	o.O.Data.Map[obj.Pair.Key] = ValueTimePair{
		val:           obj.Pair.Val,
		expireTime:    obj.Expire,
		replicateTime: time.Now().Add(tReplicate),
	}
	o.O.Data.lock.Unlock()
	*res = StoreReturn{Contact{new(big.Int).Set(o.O.ID), o.O.IP}, true}
	return nil
}

func (o *Node) RPCFindNode(arg FindNodeRequest, res *FindNodeReturn) error {
	go o.O.updateBucket(arg.Header)
	res.Header = Contact{new(big.Int).Set(o.O.ID), o.O.IP}
	res.Closest = make([]Contact, 0)
	p := distance(arg.Id, o.O.ID).BitLen() - 1
	if o.O.ID.Cmp(arg.Id) == 0 {
		p = 0
	}
	o.O.kBuckets[p].mutex.Lock()
	if o.O.kBuckets[p].size == bucketSize {
		for i := 0; i < bucketSize; i++ {
			res.Closest = append(res.Closest, o.O.kBuckets[p].arr[i])
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
		return distance(arr[i].Id, arg.Id).Cmp(distance(arr[j].Id, arg.Id)) < 0
	})
	length := len(arr)
	if length >= bucketSize {
		for i := 0; i < bucketSize; i++ {
			res.Closest = append(res.Closest, arr[i])
		}
	} else {
		res.Closest = arr
	}
	return nil
}

func (o *Node) RPCFindValue(arg FindValueRequest, res *FindValueReturn) error {
	go o.O.updateBucket(arg.Header)

	value, ok := o.O.getValue(arg.Key)
	if ok {
		*res = FindValueReturn{
			Header:  Contact{new(big.Int).Set(o.O.ID), o.O.IP},
			Closest: nil,
			Val:     value,
		}
		return nil
	}

	res.Header = Contact{new(big.Int).Set(o.O.ID), o.O.IP}
	res.Closest = make([]Contact, 0)
	res.Val = ""
	p := distance(arg.HashId, o.O.ID).BitLen() - 1
	o.O.kBuckets[p].mutex.Lock()
	if o.O.kBuckets[p].size == bucketSize {
		for i := 0; i < bucketSize; i++ {
			res.Closest = append(res.Closest, o.O.kBuckets[p].arr[i])
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
		return distance(arr[i].Id, arg.HashId).Cmp(distance(arr[j].Id, arg.HashId)) < 0
	})
	length := len(arr)
	if length >= bucketSize {
		for i := 0; i < bucketSize; i++ {
			res.Closest = append(res.Closest, arr[i])
		}
	} else {
		res.Closest = arr
	}
	return nil
}
