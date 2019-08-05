package kademlia

import (
	"crypto/sha1"
	"math/big"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Contact struct {
	Id *big.Int //server's Id
	Ip string   //server's Ip
}

type PingReturn struct {
	Header  Contact
	Success bool
}

type KVPair struct {
	Key string
	Val string
}

type StoreRequest struct {
	Header Contact
	Pair   KVPair
	Expire time.Time
}

type StoreReturn struct {
	Header  Contact
	Success bool
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
	Val     string
}

type ValueTimePair struct {
	val           string
	expireTime    time.Time
	replicateTime time.Time
}

type KVMap struct {
	Map  map[string]ValueTimePair
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
	tCheck     = time.Minute
)

func distance(x, y *big.Int) *big.Int {
	return new(big.Int).Xor(x, y)
}

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

// function Dial() to dial a given address
func Dial(addr string) (*rpc.Client, error) {
	var err error
	var client *rpc.Client
	for i := 0; i < 3; i++ {
		client, err = rpc.Dial("tcp", addr)
		if err == nil {
			return client, err
		} else {
			time.Sleep(time.Second / 2)
		}
	}
	return nil, err
}
