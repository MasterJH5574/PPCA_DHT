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
	Second           = 1000 * time.Millisecond
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
	o.Addr = GetLocalAddress() + ":" + port
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
func (o *Node) Join(addr string) bool {
	// client: the node which the current node joins from
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return false
	}

	o.Predecessor = nil
	err = client.Call("RPCNode.FindSuccessor",
		&lookupType{new(big.Int).Set(o.ID), 0}, &o.Successor[1])
	if err != nil {
		fmt.Println("Error: Calling Node.FindSuccessor: ", err)
		return false
	}

	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return false
	}

	// client: the successor of the current node
	client, err = rpc.DialHTTP("tcp", o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return false
	}

	/* ---- move k-v pairs ---- */
	var successorData *KVMap
	var successorPre *Edge

	// get successor's data(KVMap)
	err = client.Call("RPCNode.GetData", nil, &successorData)
	if err != nil {
		fmt.Println("Error: Calling Node.GetData: ", err)
		return false
	}

	// get the successor's predecessor
	cnt := 0
	for successorPre = nil; successorPre == nil && cnt < FailTimes; {
		err = client.Call("RPCNode.GetPredecessor", nil, &successorPre)
		if err != nil {
			fmt.Println("Error: Calling Node.GetPredecessor: ", err)
			return false
		}

		if successorPre == nil {
			time.Sleep(Second)
			cnt++
		} else {
			break
		}
	}
	if cnt == FailTimes {
		fmt.Printf("Error: Predecessor not found when Join\n")
		return false
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
		return false
	}

	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return false
	}

	return true
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
			time.Sleep(Second)
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
			return
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

		time.Sleep(Second)
	}
}

// method CheckPredecessor() checks whether the predecessor is failed
// called periodically, with goroutine
func (o *Node) CheckPredecessor() {
	for {
		if o.Predecessor == nil {
			return
		}
		if !o.Ping(o.Predecessor.Addr) {
			o.Predecessor = nil
		}
		time.Sleep(Second)
	}
}

// put a key into the chord ring
func (o *Node) Put(key, value string) bool {
	keyID := hashString(key)

	var res Edge
	err := o.FindSuccessor(&lookupType{new(big.Int).Set(keyID), 0}, &res)
	if err != nil {
		fmt.Println("Error: Put error: ", err)
		return false
	}

	client, err := rpc.DialHTTP("tcp", res.Addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return false
	}

	var Data *KVMap
	err = client.Call("RPCNode.GetData", nil, &Data)
	if err != nil {
		fmt.Println("Error: Calling Node.GetData: ", err)
		return false
	}

	Data.lock.Lock()
	Data.Map[key] = value // Do I need to return false?
	Data.lock.Unlock()

	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return false
	}
	return true
}

// get a key
func (o *Node) Get(key string) (string, bool) {
	keyID := hashString(key)

	var res Edge
	err := o.FindSuccessor(&lookupType{new(big.Int).Set(keyID), 0}, &res)
	if err != nil {
		fmt.Println("Error: Put error: ", err)
		return *new(string), false
	}

	client, err := rpc.DialHTTP("tcp", res.Addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return *new(string), false
	}

	var value *string
	err = client.Call("RPCNode.GetValue", nil, &value)
	if err != nil {
		fmt.Println("Error: Calling Node.GetData: ", err)
		return *new(string), false
	}
	if value == nil {
		return *new(string), false
	}

	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return *new(string), false
	}

	return *value, true
}

// delete a key
func (o *Node) Delete(key string) bool {
	keyID := hashString(key)

	var res Edge
	err := o.FindSuccessor(&lookupType{new(big.Int).Set(keyID), 0}, &res)
	if err != nil {
		fmt.Println("Error: Put error: ", err)
		return false
	}

	client, err := rpc.DialHTTP("tcp", res.Addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return false
	}

	var Data *KVMap
	err = client.Call("RPCNode.GetData", nil, &Data)
	if err != nil {
		fmt.Println("Error: Calling Node.GetData: ", err)
		return false
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return false
	}

	Data.lock.Lock()
	_, ok := Data.Map[key]
	if ok == true {
		delete(Data.Map, key)
		Data.lock.Unlock()
		return true
	}
	Data.lock.Unlock()
	return false
}
