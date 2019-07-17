/* ---- auxiliary methods ----*/

package chord

import (
	"errors"
	"fmt"
	"log"
	"math/big"
	"time"
)

// method Ping() ping the given address
func (o *Node) Ping(addr string) bool {
	return Ping(addr)
}

// method PutValue() puts a Value into the map
func (o *Node) PutValue(kv KVPair, success *bool) error {
	o.Data.lock.Lock()
	o.Data.Map[kv.Key] = kv.Value
	o.Data.lock.Unlock()
	*success = true // do i need to return false?
	return nil
}

// method GetValue() returns Value of a Key
func (o *Node) GetValue(key string, value *string) error {
	o.Data.lock.Lock()
	str, ok := o.Data.Map[key]
	o.Data.lock.Unlock()
	*value = str
	if ok == false {
		return errors.New("Get not found ")
	}
	return nil
}

// method DeleteValue() deletes a Value
func (o *Node) DeleteValue(key string, success *bool) error {
	o.Data.lock.Lock()
	_, ok := o.Data.Map[key]
	if ok == true {
		delete(o.Data.Map, key)
		*success = true
	} else {
		*success = false
	}
	o.Data.lock.Unlock()
	return nil
}

// method GetPredecessor() returns an Edge pointing to the predecessor of the current node
func (o *Node) GetPredecessor(args int, res *Edge) error {
	if o.Predecessor == nil {
		return errors.New("GetPredecessor: predecessor not found ")
	}
	*res = Edge{o.Predecessor.Addr, new(big.Int).Set(o.Predecessor.ID)}
	return nil
}

// method GetSuccessorList() returns a list of successors of a node
func (o *Node) GetSuccessorList(args int, res *[successorListLen + 1]Edge) error {
	o.sLock.Lock()
	for i := 1; i <= successorListLen; i++ {
		(*res)[i] = Edge{o.Successor[i].Addr, new(big.Int).Set(o.Successor[i].ID)}
	}
	o.sLock.Unlock()
	return nil
}

// method MoveAllDataToSuccessor(successor) moves the data of the current node to its successor
func (o *Node) MoveAllDataToSuccessor() {
	if Ping(o.Successor[1].Addr) == false {
		fmt.Println("Error: Not connected[1]")
		return
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error[1]: ", err)
		return
	}

	err = client.Call("RPCNode.QuitMoveData", o.Data, new(int))
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Calling Node.QuitMoveData: ", err)
		return
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}
}

// method MoveKVPairs() called when Join(), move successor's data to my data
func (o *Node) MoveKVPairs(newNode *big.Int, res *map[string]string) error {
	cnt := 0
	for o.Predecessor == nil && cnt < FailTimes {
		time.Sleep(Second)
		cnt++
	}
	if cnt == FailTimes {
		return errors.New("Predecessor not found when Join ")
	}

	o.Data.lock.Lock()
	for k, v := range o.Data.Map {
		KID := hashString(k)
		if between(o.Predecessor.ID, KID, newNode, true) {
			(*res)[k] = v
		}
	}
	for k := range *res {
		delete(o.Data.Map, k)
	}
	o.Data.lock.Unlock()
	return nil
}

// method QuitMoveData()
func (o *Node) QuitMoveData(Data *KVMap, res *int) error {
	Data.lock.Lock()
	o.Data.lock.Lock()
	for k, v := range Data.Map {
		o.Data.Map[k] = v
	}
	o.Data.lock.Unlock()
	Data.lock.Unlock()
	return nil
}

// method SetSuccessor()
func (o *Node) SetSuccessor(edge Edge, res *int) error {
	o.Successor[1] = edge
	var list [successorListLen + 1]Edge

	if Ping(o.Successor[1].Addr) == false {
		fmt.Println("Error: Not connected[1]")
		return errors.New("Not connected[1] ")
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error[4]: ", err)
		return err
	}
	err = client.Call("RPCNode.GetSuccessorList", 0, &list)
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Call GetSuccessorList Error", err)
		return err
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return err
	}

	o.sLock.Lock()
	for i := 2; i <= successorListLen; i++ {
		o.Successor[i] = list[i-1]
	}
	o.sLock.Unlock()
	return nil
}

// method SetPredecessor()
func (o *Node) SetPredecessor(edge Edge, res *int) error {
	o.Predecessor = &edge
	return nil
}

// method simpleStabilize() stabilize once
func (o *Node) simpleStabilize() {
	o.FixSuccessors()

	if Ping(o.Successor[1].Addr) == false {
		//fmt.Println("Error: Not connected[2]")
		return
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		//fmt.Println("Error: Dialing error[2]: ", err)
		return
	}

	var successorPre Edge
	err = client.Call("RPCNode.GetPredecessor", 0, &successorPre)
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Calling Node.GetPredecessor: ", err, o.Addr, "successor", o.Successor[1].Addr)
		return
	}

	if between(o.ID, successorPre.ID, o.Successor[1].ID, false) {
		o.sLock.Lock()
		o.Successor[1] = successorPre
		err = client.Close()
		if err != nil {
			o.sLock.Unlock()
			fmt.Println("Error: Close client error: ", err)
			return
		}

		if Ping(o.Successor[1].Addr) == false {
			fmt.Println("Error: Not connected[3]")
			return
		}
		client, err = Dial(o.Successor[1].Addr)
		o.sLock.Unlock()
		if err != nil {
			fmt.Println("Error: Dialing error[3]: ", err, o.Addr, "successorPre", successorPre.Addr)
			return
		}
	}

	err = client.Call("RPCNode.Notify", &Edge{o.Addr, new(big.Int).Set(o.ID)}, new(int))
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Node.Notify error: ", err)
		return
	}

	var list [successorListLen + 1]Edge
	err = client.Call("RPCNode.GetSuccessorList", 0, &list)
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Call GetSuccessorList Error", err)
		return
	}
	o.sLock.Lock()
	for i := 2; i <= successorListLen; i++ {
		o.Successor[i] = list[i-1]
	}
	o.sLock.Unlock()

	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}
}

// method FixSuccessors fixes the successor list
func (o *Node) FixSuccessors() {
	o.sLock.Lock()

	var p int
	for p = 1; p <= successorListLen; p++ {
		if o.Ping(o.Successor[p].Addr) {
			break
		}
	}
	if p == successorListLen+1 {
		log.Fatalln("Error: No valid successor!!!!")
	}

	if p == 1 {
		o.sLock.Unlock()
		return
	}

	o.Successor[1] = o.Successor[p]
	var list [successorListLen + 1]Edge
	if Ping(o.Successor[1].Addr) == false {
		fmt.Println("Error: Not connected[4]")
		return
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		o.sLock.Unlock()
		fmt.Println("Error: Dialing error[4]: ", err)
		return
	}

	err = client.Call("RPCNode.GetSuccessorList", 0, &list)
	if err != nil {
		o.sLock.Unlock()
		_ = client.Close()
		fmt.Println("Error: Call GetSuccessorList Error", err)
		return
	}
	err = client.Close()
	if err != nil {
		o.sLock.Unlock()
		fmt.Println("Error: Close client error: ", err)
		return
	}

	for i := 2; i <= successorListLen; i++ {
		o.Successor[i] = list[i-1]
	}
	o.sLock.Unlock()
}
