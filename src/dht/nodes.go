// Node type in chord and some methods of Node type

package chord

import (
	"errors"
	"fmt"
	"math/big"
	"net/rpc"
	"sync"
	"time"
)

const (
	M = 160
	successorListLen = 20
	timeGap = 100 * time.Millisecond
	lookupFailTimes = 32
)


// define Edge, KVMap & Node type
type Edge struct {
	Addr string
	ID big.Int
}

type KVMap struct {
	Map map[string]string
	lock sync.Mutex
}

type Node struct {
	Addr string
	ID big.Int

	Successor [M + 1]Edge
	Predecessor *Edge
	Finger [successorListLen]Edge

	Data KVMap // map with mutex lock
}


// define lookup type
type lookupType struct {
	ID big.Int
	cnt int
}

// method init() initialize the node
func (o *Node) Init(port string) {
	o.Addr = getLocalAddress() + ":" + port
	o.ID = hashString(o.Addr)
	o.Data.Map = make(map[string]string)
}

// method FindSuccessor returns an edge pointing to the successor of ID in pos
// this method may be called by other goroutine
func (o *Node) FindSuccessor(pos *lookupType, res *Edge) error {
	if pos.cnt > lookupFailTimes {
		return errors.New("Lookup failure: not found ")
	}
	return nil
	// TODO: finish FindSuccessor
}
// TODO: need func closest_preceding_node(id)?

// method Create() creates a new chord ring
func (o *Node) Create() {
	o.Predecessor = nil
	o.Successor[1] = Edge{o.Addr, o.ID}
}

// method Join() make a node p join the chord ring
func (o *Node) Join(addr string) {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		fmt.Printf("Error: Dialing error: ")
		fmt.Println(err)
		return
	}

	err = client.Call("Node.FindSuccessor", &lookupType{o.ID, 0}, &o.Successor[1])
	if err != nil {
		fmt.Printf("Error: Calling Node.FindSuccessor: ")
		fmt.Println(err)
		return
	}

	

	// TODO: finish Join
}

// method Stabilize() maintain the current successor of node o
// called periodically, with goroutine
func (o *Node) Stabilize() {
	// TODO: finish Stabilize
}

// method Notify() update the predecessor of node p
// note that node o is the predecessor of node p
// called when o.stabilize()
func (o *Node) Notify() {
	// TODO: finish Notify
}

// method FixFingers() maintains the FingerTable of node o
// called periodically, with goroutine
func (o *Node) FixFingers() {
	// TODO: finish FixFingers
}

// method CheckPredecessor() checks whether the predecessor is failed
// called periodically, with goroutine
func (o *Node) CheckPredecessor() {
	// TODO: finish CheckPredecessor
}