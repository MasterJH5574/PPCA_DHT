// some useful functions

package chord

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"time"
)

var (
	two     = big.NewInt(2)
	hashMod = new(big.Int).Exp(two, big.NewInt(M), nil)
)

// hash functions
func hashString(elt string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(elt))
	return new(big.Int).SetBytes(hash.Sum(nil))
}

// used to calculate the destination of finger entries
func jump(n *big.Int, power int) *big.Int {
	gap := new(big.Int).Exp(two, big.NewInt(int64(power)-1), nil)
	res := new(big.Int).Add(n, gap)
	return new(big.Int).Mod(res, hashMod)
}

// check whether elt is between start and end
// if inclusive == true, it tests if elt is in (start, end]
// otherwise it tests if elt is in (start, end)
func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
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

// function Dial to dial a given address
func Dial(addr string) (*rpc.Client, error) {
	var res error
	for i := 0; i < 10; i++ {
		client, err := rpc.Dial("tcp", addr)
		if err == nil {
			return client, err
		} else {
			fmt.Println("Dial waiting...")
		}
		res = err
		time.Sleep(Second)
	}
	return nil, res
}
