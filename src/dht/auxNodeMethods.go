/* ---- auxiliary methods ----*/

package chord

import (
	"fmt"
	"math/big"
	"net/rpc"
)

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

// method MoveAllDataToSuccessor(successor) moves the data of the current node to its successor
func (o *Node) MoveAllDataToSuccessor() {
	client, err := rpc.DialHTTP("tcp", o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return
	}

	var successorData *KVMap
	err = client.Call("RPCNode.GetData", nil, &successorData)
	if err != nil {
		fmt.Println("Error: Calling Node.GetData: ", err)
		return
	}

	successorData.lock.Lock()
	o.Data.lock.Lock()
	for k, v := range o.Data.Map {
		successorData.Map[k] = v
	}
	o.Data.lock.Unlock()
	successorData.lock.Unlock()

	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}
}

// method SetSuccessor()
func (o *Node) SetSuccessor(edge Edge, res interface{}) error {
	o.Successor[1] = edge
	return nil
}

// method SetPredecessor()
func (o *Node) SetPredecessor(edge Edge, res interface{}) error {
	o.Predecessor = &edge
	return nil
}

// method simpleStabilize() stabilize once
func (o *Node) simpleStabilize() {
	client, err := rpc.DialHTTP("tcp", o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error: ", err)
		return
	}

	var successorPre *Edge
	err = client.Call("RPCNode.GetPredecessor", nil, &successorPre)
	if err != nil {
		fmt.Println("Error: Calling Node.GetPredecessor: ", err)
		return
	}

	if between(o.ID, successorPre.ID, o.Successor[1].ID, false) {
		o.Successor[1] = *successorPre
		err = client.Close()
		if err != nil {
			fmt.Println("Error: Close client error: ", err)
			return
		}

		client, err = rpc.DialHTTP("tcp", o.Successor[1].Addr)
		if err != nil {
			fmt.Println("Error: Dialing error: ", err)
			return
		}
	}
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
