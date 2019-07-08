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
	M                = 160
	successorListLen = 20
	timeGap          = 1000 * time.Millisecond
	FailTimes        = 32
)

// define Edge, KVMap & Node type
type Edge struct {
	Addr string
	ID   *big.Int
}

type KVMap struct {
	Map  map[string]string
	lock sync.Mutex
}

type Node struct {
	Addr string
	ID   *big.Int

	Successor   [M + 1]Edge
	Predecessor *Edge
	Finger      [successorListLen]Edge

	Data KVMap // map with mutex lock
}

// define lookup type
type lookupType struct {
	ID  *big.Int
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
	if pos.cnt >= FailTimes {
		return errors.New("Lookup failure: not found ")
	}
	return nil
	// TODO: finish FindSuccessor
}

// TODO: need func closest_preceding_node(id)?

// method Create() creates a new chord ring
// Note that the predecessor of the only node is itself
func (o *Node) Create() {
	o.Predecessor = &Edge{o.Addr, new(big.Int).Set(o.ID)}
	o.Successor[1] = Edge{o.Addr, new(big.Int).Set(o.ID)}
}

// method Join() make a node p join the chord ring
func (o *Node) Join(addr string) {
	// client: the node which the current node joins from
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		fmt.Printf("Error: Dialing error: ")
		fmt.Println(err)
		return
	}

	o.Predecessor = nil
	err = client.Call("Node.FindSuccessor",
		&lookupType{new(big.Int).Set(o.ID), 0}, &o.Successor[1])
	if err != nil {
		fmt.Printf("Error: Calling Node.FindSuccessor: ")
		fmt.Println(err)
		return
	}

	// client: the successor of the current node
	client, err = rpc.DialHTTP("tcp", o.Successor[1].Addr)
	if err != nil {
		fmt.Printf("Error: Dialing error: ")
		fmt.Println(err)
		return
	}

	/* ---- move k-v pair ---- */
	var successorData *KVMap
	var successorPre *Edge

	// get successor's data(KVMap)
	err = client.Call("Node.GetData", nil, &successorData)
	if err != nil {
		fmt.Printf("Error: Calling Node.GetData: ")
		fmt.Println(err)
		return
	}

	// get the successor's predecessor
	cnt := 0
	for successorPre = nil; successorPre == nil && cnt < FailTimes; {
		err = client.Call("Node.GetPredecessor", nil, &successorPre)
		if err != nil {
			fmt.Printf("Error: Calling Node.GetPredecessor: ")
			fmt.Println(err)
			return
		}

		if successorPre == nil {
			time.Sleep(timeGap)
			cnt++
		} else {
			break
		}
	}
	if cnt == FailTimes {
		fmt.Printf("Error: Predecessor not found when Join\n")
		return
	}

	successorData.lock.Lock()
	o.Data.lock.Lock()
	for k, v := range successorData.Map {
		KID := hashString(k)
		if between(successorPre.ID, KID, o.ID, true) {
			o.Data.Map[k] = v
		}
	}
	for k := range o.Data.Map {
		delete(successorData.Map, k)
	}
	o.Data.lock.Unlock()
	successorData.lock.Unlock()
	/* ---- finish move k-v pair */

	// Notify the successor of the current node
	err = client.Call("Node.Notify", &Edge{o.Addr, new(big.Int).Set(o.ID)}, nil)
}

// method Stabilize() maintain the current successor of node o
// called periodically, with goroutine
func (o *Node) Stabilize() {
	// TODO: finish Stabilize
}

// method Notify() update the predecessor of node p
// note that node o is the predecessor of node p
// called when o.stabilize()
func (o *Node) Notify(pred *Edge, res interface{}) error {
	if o.Predecessor == nil || between(o.Predecessor.ID, pred.ID, o.ID, false) {
		o.Predecessor = pred
	}
	return nil
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

/* ---- auxiliary methods ----*/
// method GetData() returns KVMap Data of the current node
func (o *Node) GetData(args interface{}, res **KVMap) error {
	*res = &o.Data
	return nil
}

// method GetPredecessor() returns an Edge pointing to the predecessor of the current node
func (o *Node) GetPredecessor(args interface{}, res **Edge) error {
	*res = o.Predecessor
	return nil
}
