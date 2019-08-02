package kademlia

import (
	"crypto/sha1"
	"math/big"
	"net"
	"sync"
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

type KVMap struct {
	Map  map[string]string
	lock sync.Mutex
}

const (
	bucketSize = 20
	ALPHA      = 3
	B          = 160
)

const (
	tExpire    = 24*time.Hour + 10*time.Second
	tRepublish = 24 * time.Hour
	tRefresh   = time.Hour
	tReplicate = time.Hour
)

// hash functions
func hashString(elt string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(elt))
	return new(big.Int).SetBytes(hash.Sum(nil))
}

// function to get local address(ip address)
func GetLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				ipnet, ok := addr.(*net.IPNet)
				if ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}
