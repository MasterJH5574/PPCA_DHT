package kademlia

import "math/big"

type kBucket struct {
	size int
	arr  [bucketSize]Contact
}

type node struct {
	IP string
	ID *big.Int

	kBuckets [B]kBucket
	Data     KVMap
}

type Node struct {
	O node
}
