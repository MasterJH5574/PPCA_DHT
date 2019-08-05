package kademlia

import (
	"fmt"
	"math/big"
	"net/rpc"
	"sort"
	"time"
)

type node struct {
	IP string
	ID *big.Int

	kBuckets [B]kBucket
	Data     KVMap
}

type Node struct {
	O node
}

func (o *node) Ping(addr string) bool {
	var success bool
	for i := 0; i < 3; i++ {
		chOK := make(chan bool)
		go func() {
			client, err := rpc.Dial("tcp", addr)
			if err == nil {
				err = client.Close()
				chOK <- true
			} else {
				chOK <- false
			}
		}()
		select {
		case ok := <-chOK:
			if ok {
				success = true
			} else {
				continue
			}
		case <-time.After(time.Second / 2):
			break
		}
		if success == true {
			break
		}
	}
	if success == false {
		return false
	}
	client, err := Dial(addr)
	if err != nil {
		fmt.Println("Error:", err)
		return false
	}
	var res PingReturn
	err = client.Call("Node.RPCPing", Contact{new(big.Int).Set(o.ID), o.IP}, &res)
	_ = client.Close()
	if err != nil {
		fmt.Println("Error:", err)
		return false
	}
	if res.Success == true {
		go o.updateBucket(res.Header)
	}
	return res.Success
}

func (o *node) updateBucket(t Contact) {
	k := distance(o.ID, t.Id).BitLen() - 1
	o.kBuckets[k].update(t)
}

func (o *node) getValue(key string) (string, bool) {
	o.Data.lock.Lock()
	defer o.Data.lock.Unlock()

	val, ok := o.Data.Map[key]
	return val.val, ok
}

func (o *node) getAlphaNodes(id *big.Int) []Contact {
	var res []Contact
	p := distance(o.ID, id).BitLen() - 1
	o.kBuckets[p].mutex.Lock()
	if o.kBuckets[p].size >= ALPHA {
		for i := 0; i < ALPHA; i++ {
			res = append(res, o.kBuckets[p].arr[i])
		}
		o.kBuckets[p].mutex.Unlock()
		return res
	}
	o.kBuckets[p].mutex.Unlock()
	var arr []Contact
	for i := 0; i < B; i++ {
		o.kBuckets[i].mutex.Lock()
		for j := 0; j < o.kBuckets[i].size; j++ {
			arr = append(arr, o.kBuckets[i].arr[j])
		}
		o.kBuckets[i].mutex.Unlock()
	}
	sort.Slice(arr, func(i, j int) bool {
		return distance(arr[i].Id, id).Cmp(distance(arr[j].Id, id)) < 0
	})
	length := len(arr)
	if length >= ALPHA {
		for i := 0; i < ALPHA; i++ {
			res = append(res, arr[i])
		}
	} else {
		res = arr
	}
	return res
}

func (o *node) iterativeFindNode(id *big.Int) []Contact {
	var arr []Contact
	MAP := make(map[string]bool)

	que := o.getAlphaNodes(new(big.Int).Set(id))
	head := 0
	for head < len(que) {
		if MAP[que[head].Ip] == true {
			head++
			continue
		}
		if o.Ping(que[head].Ip) == true {
			MAP[que[head].Ip] = true
			arr = append(arr, que[head])

			client, err := Dial(que[head].Ip)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			var res FindNodeReturn
			err = client.Call("Node.RPCFindNode", FindNodeRequest{
				Header: Contact{new(big.Int).Set(o.ID), o.IP},
				Id:     id,
			}, &res)
			_ = client.Close()
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			go o.updateBucket(res.Header)
			for _, v := range res.Closest {
				que = append(que, v)
			}
		}
		head++
	}
	sort.Slice(arr, func(i, j int) bool {
		return distance(arr[i].Id, id).Cmp(distance(arr[j].Id, id)) < 0
	})
	if len(arr) >= bucketSize {
		var res []Contact
		for i := 0; i < bucketSize; i++ {
			res = append(res, arr[i])
		}
		return res
	} else {
		return arr
	}
}

func (o *node) iterativeFindValue(arg FindValueRequest) (string, bool) {
	var arr []Contact
	MAP := make(map[string]bool)

	que := o.getAlphaNodes(new(big.Int).Set(arg.HashId))
	head := 0
	for head < len(que) {
		if MAP[que[head].Ip] == true {
			head++
			continue
		}
		if o.Ping(que[head].Ip) == true {
			MAP[que[head].Ip] = true

			client, err := Dial(que[head].Ip)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			var res FindValueReturn
			err = client.Call("Node.RPCFindValue", FindValueRequest{
				Header: Contact{new(big.Int).Set(o.ID), o.IP},
				HashId: arg.HashId,
				Key:    arg.Key,
			}, &res)
			_ = client.Close()
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			go o.updateBucket(res.Header)

			if res.Closest == nil { // already get the value
				sort.Slice(arr, func(i, j int) bool {
					return distance(arr[i].Id, arg.HashId).Cmp(distance(arr[j].Id, arg.HashId)) < 0
				})
				if len(arr) > 0 { // for caching
					go func() {
						client, err := Dial(arr[0].Ip)
						if err != nil {
							fmt.Println("Error:", err)
						}
						var storeReturn StoreReturn
						err = client.Call("Node.RPCStore", StoreRequest{
							Header: Contact{new(big.Int).Set(o.ID), o.IP},
							Pair:   KVPair{arg.Key, res.Val},
							Expire: time.Now().Add(tExpire),
						}, &storeReturn)
						_ = client.Close()
						if err != nil {
							fmt.Println("Error:", err)
						}
						go o.updateBucket(storeReturn.Header)
					}()
				}
				return res.Val, true
			} else { // value not found so far
				for _, v := range res.Closest {
					que = append(que, v)
				}
				arr = append(arr, que[head])
			}
		}
		head++
	}
	return "", false
}

func (o *node) iterativeStore(arg StoreRequest) bool {
	hash := hashString(arg.Pair.Key)
	closest := o.iterativeFindNode(new(big.Int).Set(hash))
	success := false
	for _, t := range closest {
		client, err := Dial(t.Ip)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		var res StoreReturn
		err = client.Call("Node.RPCStore", StoreRequest{
			Header: Contact{new(big.Int).Set(o.ID), o.IP},
			Pair:   arg.Pair,
			Expire: time.Now().Add(tExpire),
		}, &res)
		_ = client.Close()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		go o.updateBucket(res.Header)
		if res.Success == true {
			success = true
		}
	}
	return success
}
