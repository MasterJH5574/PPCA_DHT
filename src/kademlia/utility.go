package kademlia

import (
	"math/big"
	"time"
)

type Contact struct {
	Id *big.Int //server's Id
	Ip string   //server's Ip
}

type PingReturn struct {
	Success bool
	Header  Contact
}

type KVPair struct {
	Key string
	Val string
}

type StoreRequest struct {
	Pair      KVPair
	Header    Contact
	Expire    time.Time
	Replicate bool
}

type StoreReturn struct {
	Success bool
	Header  Contact
}

type FindNodeRequest struct {
	Header Contact
	Id     *big.Int
}

type FindNodeReturn struct {
	Header  Contact
	Closest []Contact
}

type FindValueRequest struct {
	Header Contact
	HashId *big.Int
	Key    string
}

type Set map[string]struct{}

type FindValueReturn struct {
	Header  Contact
	Closest []Contact
	Val     Set
}
