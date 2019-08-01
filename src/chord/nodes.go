// Node type in chord and main methods of Node type

package chord

import (
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"
)

const (
	M                = 160
	successorListLen = M
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

type KVPair struct {
	Key, Value string
}

type Node struct {
	Addr string
	ID   *big.Int

	Successor [successorListLen + 1]Edge
	sLock     sync.Mutex

	Predecessor *Edge
	Finger      [M + 1]Edge

	Data    KVMap // map with mutex lock
	DataPre KVMap

	FingerIndex int
	ON          bool
}

// define lookup type
type LookupType struct {
	ID  *big.Int
	cnt int
}

// method init() initialize the node
func (o *Node) Init(port string) {
	o.Addr = GetLocalAddress() + ":" + port
	o.ID = hashString(o.Addr)
	o.Data.Map = make(map[string]string)
	o.DataPre.Map = make(map[string]string)
}

// method FindSuccessor returns an edge pointing to the successor of ID in pos
// this method may be called by other goroutine
func (o *Node) FindSuccessor(pos *LookupType, res *Edge) error {
	pos.cnt++
	if pos.cnt >= FailTimes {
		return errors.New("Lookup failure: not found ")
	}
	err := o.FixSuccessors()
	if err != nil {
		return err
	}
	if o.Successor[1].Addr == o.Addr || pos.ID.Cmp(o.ID) == 0 {
		*res = Edge{o.Addr, new(big.Int).Set(o.ID)}
	} else if between(o.ID, pos.ID, o.Successor[1].ID, true) {
		*res = Edge{o.Successor[1].Addr, new(big.Int).Set(o.Successor[1].ID)}
	} else {
		nextNode := o.closestPrecedingNode(pos.ID)
		if nextNode.ID == nil {
			fmt.Println("nextNode not found, waiting...")
			time.Sleep(Second / 2)
			return o.FindSuccessor(pos, res)
		}

		if Ping(nextNode.Addr) == false {
			fmt.Println("Error: Not connected(1)")
			return errors.New("Not connected(1) ")
		}
		client, err := Dial(nextNode.Addr)
		if err != nil {
			fmt.Println("Error: Dialing error(1): ", err)
			return err
		}

		err = client.Call("RPCNode.FindSuccessor", pos, res)
		if err != nil {
			_ = client.Close()
			fmt.Println("Error: Find successor Calling Node.FindSuccessor:", err)
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
func (o *Node) closestPrecedingNode(id *big.Int) Edge {
	for i := M; i > 0; i-- {
		if o.Finger[i].ID != nil && o.Ping(o.Finger[i].Addr) {
			if between(o.ID, o.Finger[i].ID, id, true) {
				return Edge{o.Finger[i].Addr, new(big.Int).Set(o.Finger[i].ID)}
			}
		}
	}
	_ = o.FixSuccessors()
	if o.Ping(o.Successor[1].Addr) {
		return Edge{o.Successor[1].Addr, new(big.Int).Set(o.Successor[1].ID)}
	} else {
		return Edge{"", new(big.Int)}
	}
}

// method Create() creates a new chord ring
// Note that the predecessor of the only node is itself
func (o *Node) Create() {
	o.Predecessor = &Edge{o.Addr, new(big.Int).Set(o.ID)}
	for i := 1; i <= successorListLen; i++ {
		o.Successor[i] = Edge{o.Addr, new(big.Int).Set(o.ID)}
	}
}

// method Join() make a node p join the chord ring
func (o *Node) Join(addr string) bool {
	// client: the node which the current node joins from
	if Ping(addr) == false {
		fmt.Println("Error: Not connected(2)")
		return false
	}
	client, err := Dial(addr)
	if err != nil {
		fmt.Println("Error: Dialing error(2): ", err)
		return false
	}

	o.Predecessor = nil
	err = client.Call("RPCNode.FindSuccessor",
		&LookupType{new(big.Int).Set(o.ID), 0}, &o.Successor[1])
	if err != nil {
		_ = client.Close()
		log.Fatalln("Error: Calling Node.FindSuccessor: ", err)
		return false
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return false
	}

	// client: the successor of the current node
	if Ping(o.Successor[1].Addr) == false {
		fmt.Println("Error: Not connected(3)")
		return false
	}
	client, err = Dial(o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error(3): ", err)
		return false
	}

	var list [successorListLen + 1]Edge
	err = client.Call("RPCNode.GetSuccessorList", 0, &list)
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Call GetSuccessorList Error", err)
		return false
	}
	o.sLock.Lock()
	for i := 2; i <= successorListLen; i++ {
		o.Successor[i] = list[i-1]
	}
	o.sLock.Unlock()

	/* ---- move k-v pairs ---- */
	o.DataPre.lock.Lock()
	err = client.Call("RPCNode.MoveDataPre", 0, &o.DataPre.Map)
	o.DataPre.lock.Unlock()
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: MoveDataPre", err)
		return false
	}

	o.Data.lock.Lock()
	err = client.Call("RPCNode.MoveKVPairs", new(big.Int).Set(o.ID), &o.Data.Map)
	o.Data.lock.Unlock()
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: MoveKVPairs", err)
		return false
	}

	// Notify the successor of the current node
	err = client.Call("RPCNode.Notify", &Edge{o.Addr, new(big.Int).Set(o.ID)}, new(int))
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Node.Notify error: ", err)
		return false
	}

	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return false
	}

	time.Sleep(200 * time.Millisecond)
	return true
}

// method Quit() let the current node quit the chord ring
// note that the current node has predecessor and successor
func (o *Node) Quit() {
	err := o.FixSuccessors()
	if err != nil {
		return
	}

	if o.Successor[1].Addr == o.Addr {
		fmt.Println("Quit success")
		return
	}
	o.MoveAllDataToSuccessor()

	// set the predecessor's successor
	if Ping(o.Predecessor.Addr) == false {
		fmt.Println("Error: Not connected(4)")
		return
	}
	client, err := Dial(o.Predecessor.Addr)
	if err != nil {
		fmt.Println("Error: Dialing error(4): ", err)
		return
	}
	err = client.Call("RPCNode.SetSuccessor", o.Successor[1], new(int))
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Node.SetSuccessor error: ", err)
		return
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}

	// set the successor's predecessor
	if Ping(o.Successor[1].Addr) == false {
		fmt.Println("Error: Not connected(5)")
		return
	}
	client, err = Dial(o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error(5): ", err)
		return
	}
	err = client.Call("RPCNode.SetPredecessor", *o.Predecessor, new(int))
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Node.SetPredecessor error: ", err)
		return
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}

	o.ON = false
	fmt.Println("Quit success")
}

// method Stabilize() maintain the current successor of node o
// called periodically, with goroutine
func (o *Node) Stabilize(infinite bool) {
	if infinite == false {
		o.simpleStabilize()
	} else {
		for o.ON == true {
			o.simpleStabilize()
			time.Sleep(Second / 4)
		}
	}
}

// method Notify() update the predecessor of node p
// note that node o is the predecessor of node p
// called when o.stabilize()
func (o *Node) Notify(pred *Edge, res *int) error {
	oldPre := o.Predecessor
	if o.Predecessor == nil || between(o.Predecessor.ID, pred.ID, o.ID, false) {
		o.Predecessor = pred
	}
	if oldPre != o.Predecessor && o.Predecessor != nil {
		if o.Predecessor.Addr != o.Addr {
			if Ping(o.Predecessor.Addr) == false {
				return errors.New("Error: Not connected(9) ")
			}
			client, err := Dial(o.Predecessor.Addr)
			if err != nil {
				return err
			}
			o.DataPre.lock.Lock()
			o.DataPre.Map = make(map[string]string)
			err = client.Call("RPCNode.MoveDataPre", 1, &o.DataPre.Map)
			o.DataPre.lock.Unlock()
			err = client.Close()
			if err != nil {
				return err
			}
		} else {
			o.DataPre.lock.Lock()
			o.Data.lock.Lock()
			o.DataPre.Map = o.Data.Map
			o.Data.lock.Unlock()
			o.DataPre.lock.Unlock()
		}
	}
	return nil
}

// method FixFingers() maintains the FingerTable of node o
// called periodically, with goroutine
func (o *Node) FixFingers() {
	o.FingerIndex = 1
	for o.ON == true {
		if o.Successor[1].Addr != o.Finger[1].Addr {
			o.FingerIndex = 1
		}

		var lookup LookupType
		for i := 0; i < 5; i++ {
			lookup = LookupType{jump(o.ID, o.FingerIndex), 0}
			err := o.FindSuccessor(&lookup, &o.Finger[o.FingerIndex])
			if err == nil {
				break
			} else if i == 4 {
				fmt.Println("Error: FixFingers error, quit FixFingers,", o.Addr)
				return
			}
			fmt.Println("Fix finger waiting...", i)
			time.Sleep(Second / 4)
		}

		edge := o.Finger[o.FingerIndex]

		o.FingerIndex++
		if o.FingerIndex > M {
			o.FingerIndex = 1
			continue
		}

		for {
			if between(o.ID, jump(o.ID, o.FingerIndex), edge.ID, true) {
				o.Finger[o.FingerIndex] = Edge{edge.Addr, new(big.Int).Set(edge.ID)}
				o.FingerIndex++
				if o.FingerIndex > M {
					o.FingerIndex = 1
					break
				}
			} else {
				break
			}
		}

		time.Sleep(Second / 4)
	}
}

// method CheckPredecessor() checks whether the predecessor is failed
// called periodically, with goroutine
func (o *Node) CheckPredecessor() {
	for o.ON == true {
		if o.Predecessor == nil {
			time.Sleep(Second / 4)
			continue
		}
		if !o.Ping(o.Predecessor.Addr) {
			fmt.Println(o.Addr, "predecessor:", o.Predecessor.Addr, "-> nil")
			o.Predecessor = nil

			_ = o.FixSuccessors()
			if o.Successor[1].Addr != o.Addr {
				if !Ping(o.Successor[1].Addr) {
					fmt.Println("Error: Not connected(10)")
					continue
				}
				client, err := Dial(o.Successor[1].Addr)
				if err != nil {
					fmt.Println(err)
					continue
				}
				o.DataPre.lock.Lock()
				o.Data.lock.Lock()
				for k, v := range o.DataPre.Map {
					o.Data.Map[k] = v
					err = client.Call("RPCNode.PutValueDataPre", KVPair{k, v}, new(bool))
					if err != nil {
						o.Data.lock.Unlock()
						o.DataPre.lock.Unlock()
						_ = client.Close()
						fmt.Println(err)
					}
				}
				o.DataPre.Map = make(map[string]string)
				o.Data.lock.Unlock()
				o.DataPre.lock.Unlock()
			} else {
				o.DataPre.lock.Lock()
				o.Data.lock.Lock()
				for k, v := range o.DataPre.Map {
					o.Data.Map[k] = v
				}
				o.DataPre.Map = make(map[string]string)
				o.Data.lock.Unlock()
				o.DataPre.lock.Unlock()
			}
		}
		time.Sleep(Second / 4)
	}
}

// put a Key into the chord ring
func (o *Node) Put(key, value string) bool {
	time.Sleep(15 * time.Millisecond)
	keyID := hashString(key)

	var res Edge
	err := o.FindSuccessor(&LookupType{new(big.Int).Set(keyID), 0}, &res)
	if err != nil {
		fmt.Println("Error: Put error: ", err)
		return false
	}

	if Ping(res.Addr) == false {
		fmt.Println("Error: Not connected(6)")
		return false
	}
	client, err := Dial(res.Addr)
	if err != nil {
		fmt.Println("Error: Dialing error(6): ", err)
		return false
	}

	var success bool
	err = client.Call("RPCNode.PutValue", KVPair{key, value}, &success)
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Calling Node.PutValue: ", err)
		return false
	}
	err = client.Call("RPCNode.PutValueSuccessor", KVPair{key, value}, new(bool))
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Calling Node.PutValueSuccessor: ", err)
		return false
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return false
	}

	fmt.Println("Put at", res.Addr, ": Key =", key, "Value =", value)
	return success
}

// get a Key
func (o *Node) Get(key string) (string, bool) {
	time.Sleep(15 * time.Millisecond)
	keyID := hashString(key)

	for i := 0; i < 5; i++ {
		var res Edge
		err := o.FindSuccessor(&LookupType{new(big.Int).Set(keyID), 0}, &res)
		if err != nil {
			fmt.Println("Error: Get error: ", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if Ping(res.Addr) == false {
			fmt.Println("Error: Not connected(7)")
			time.Sleep(200 * time.Millisecond)
			continue
		}
		client, err := Dial(res.Addr)
		if err != nil {
			fmt.Println("Error: Dialing error(7): ", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var value string
		err = client.Call("RPCNode.GetValue", key, &value)
		if err != nil {
			err = client.Close()
			if err != nil {
				fmt.Println("Error: Close client error: ", err)
				time.Sleep(200 * time.Millisecond)
				continue
			}

			//fmt.Println("Get not found at", res.Addr, ": Key =", key)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		_ = client.Close()

		fmt.Println("Get at", res.Addr, ": Key =", key, "Value =", value)
		return value, true
	}

	fmt.Println("Get not found: Key =", key)
	return "", false
}

// delete a Key
func (o *Node) Delete(key string) bool {
	time.Sleep(15 * time.Millisecond)
	keyID := hashString(key)

	var res Edge
	err := o.FindSuccessor(&LookupType{new(big.Int).Set(keyID), 0}, &res)
	if err != nil {
		fmt.Println("Error: Delete error: ", err)
		return false
	}

	if Ping(res.Addr) == false {
		fmt.Println("Error: Not connected(8)")
		return false
	}
	client, err := Dial(res.Addr)
	if err != nil {
		fmt.Println("Error: Dialing error(8): ", err)
		return false
	}

	var success bool
	err = client.Call("RPCNode.DeleteValue", key, &success)
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Calling Node.DeleteValue: ", err)
		return false
	}
	err = client.Call("RPCNode.DeleteValueSuccessor", key, new(bool))
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Calling Node.DeleteValueSuccessor: ", err)
		return false
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return false
	}

	if success == true {
		fmt.Println("Delete success at", res.Addr, ": Key =", key)
		return true
	}
	fmt.Println("Delete not found at", res.Addr, ": Key =", key)
	return false
}

// method Dump()
func (o *Node) Dump() {
	fmt.Println("---------- DUMP ----------")
	fmt.Println("Addr:", o.Addr)
	fmt.Println("ID:", o.ID)
	fmt.Println("Successor:", o.Successor)
	fmt.Println("Finger Table:", o.Finger)

	if o.Predecessor == nil {
		fmt.Println("Predecessor: nil")
	} else {
		fmt.Println("Predecessor:", o.Predecessor)
	}

	o.Data.lock.Lock()
	fmt.Println("K-V pairs:", o.Data.Map)
	o.Data.lock.Unlock()
	o.DataPre.lock.Lock()
	fmt.Println("DataPre  :", o.DataPre.Map)
	o.DataPre.lock.Unlock()
	fmt.Println("-------- DUMP END --------")
}
