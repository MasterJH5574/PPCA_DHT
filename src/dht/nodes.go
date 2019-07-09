// Node type in chord and main methods of Node type

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

	FingerIndex int
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
	if o.Successor[1].Addr == o.Addr || pos.ID.Cmp(o.ID) == 0 {
		*res = Edge{o.Addr, new(big.Int).Set(o.ID)}
	} else if between(o.ID, pos.ID, o.Successor[1].ID, true) {
		*res = Edge{o.Successor[1].Addr, new(big.Int).Set(o.Successor[1].ID)}
	} else {
		nextNode := o.closestPrecedingNode(new(big.Int).Set(pos.ID))

		client, err := rpc.DialHTTP("tcp", nextNode.Addr)
		if err != nil {
			fmt.Println("Error: Dialing error: ", err)
			return err
		}

		err = client.Call("RPCNode.FindSuccessor", pos, res)
		if err != nil {
			fmt.Println("Error: Calling Node.FindSuccessor: ", err)
			return err
		}
		err = client.Close()
		if err != nil {
			fmt.Println("Error: Close client error: ", err)
			return err
		}
	}
	return nil
}

// method closestPrecedingNode() searches the local table for the highest predecessor of id
func (o *Node) closestPrecedingNode(id *big.Int) *Edge {
	for i := M; i > 0; i-- {
		if between(o.ID, o.Finger[i].ID, id, true) {
			return &Edge{o.Finger[i].Addr, o.Finger[i].ID}
		}
	}
	return &Edge{o.Successor[1].Addr, o.Successor[1].ID}
}

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
		fmt.Println("Error: Dialing error: ", err)
		return
	}

	o.Predecessor = nil
	err = client.Call("RPCNode.FindSuccessor",
		&lookupType{new(big.Int).Set(o.ID), 0}, &o.Successor[1])
	if err != nil {
		fmt.Println("Error: Calling Node.FindSuccessor: ", err)
		return
	}

	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}

	// client: the successor of the current node
	client, err = rpc.DialHTTP("tcp", o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return
	}

	/* ---- move k-v pairs ---- */
	var successorData *KVMap
	var successorPre *Edge

	// get successor's data(KVMap)
	err = client.Call("RPCNode.GetData", nil, &successorData)
	if err != nil {
		fmt.Println("Error: Calling Node.GetData: ", err)
		return
	}

	// get the successor's predecessor
	cnt := 0
	for successorPre = nil; successorPre == nil && cnt < FailTimes; {
		err = client.Call("RPCNode.GetPredecessor", nil, &successorPre)
		if err != nil {
			fmt.Println("Error: Calling Node.GetPredecessor: ", err)
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
	err = client.Call("RPCNode.Notify", &Edge{o.Addr, new(big.Int).Set(o.ID)}, nil)
	if err != nil {
		fmt.Println("Error: Node.Notify error: ", err)
		return
	}

	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}
}

// method Quit() let the current node quit the chord ring
// note that the current node has predecessor and successor
func (o *Node) Quit() {
	o.MoveAllDataToSuccessor()

	// set the predecessor's successor
	client, err := rpc.DialHTTP("tcp", o.Predecessor.Addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return
	}
	err = client.Call("RPCNode.SetSuccessor", o.Successor[1], nil)
	if err != nil {
		fmt.Println("Error: Node.SetSuccessor error: ", err)
		return
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}

	// set the successor's predecessor
	client, err = rpc.DialHTTP("tcp", o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return
	}
	err = client.Call("RPCNode.SetPredecessor", *o.Predecessor, nil)
	if err != nil {
		fmt.Println("Error: Node.SetPredecessor error: ", err)
		return
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}
}

// method Stabilize() maintain the current successor of node o
// called periodically, with goroutine
func (o *Node) Stabilize(infinite bool) {
	if infinite == false {
		o.simpleStabilize()
	} else {
		for {
			o.simpleStabilize()
			time.Sleep(timeGap)
		}
	}
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
	o.FingerIndex = 1
	for {
		err := o.FindSuccessor(&lookupType{jump(o.Addr, o.FingerIndex), 0}, &o.Finger[o.FingerIndex])
		if err != nil {
			fmt.Println("Error: FixFingers Error: ", err)
			return
		}

		edge := o.Finger[o.FingerIndex]

		o.FingerIndex++
		if o.FingerIndex > M {
			o.FingerIndex = 1
		}

		for {
			if between(o.ID, jump(o.Addr, o.FingerIndex), edge.ID, true) {
				o.Finger[o.FingerIndex] = edge
				o.FingerIndex++
				if o.FingerIndex > M {
					o.FingerIndex = 1
					break
				}
			} else {
				break
			}
		}

		time.Sleep(timeGap)
	}
}

// method CheckPredecessor() checks whether the predecessor is failed
// called periodically, with goroutine
func (o *Node) CheckPredecessor() {
	for {
		if o.Predecessor == nil {
			return
		}
		client, err := rpc.DialHTTP("tcp", o.Predecessor.Addr)
		if err != nil {
			o.Predecessor = nil
		} else {
			err = client.Close()
			if err != nil {
				fmt.Println("Error: Close client error: ", err)
				return
			}
		}
		time.Sleep(timeGap)
	}
}

/* method used for rpc call:
FindSuccessor
Notify
GetData
GetPredecessor
SetSuccessor
SetPredecessor
*/
