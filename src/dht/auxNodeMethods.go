/* ---- auxiliary methods ----*/

package chord

import (
	"fmt"
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
	err = client.Call("Node.GetData", nil, &successorData)
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
func (o *Node) SetSuccessor(edge Edge) {
	o.Successor[1] = edge
}

// method SetPredecessor()
func (o *Node) SetPredecessor(edge Edge) {
	o.Predecessor = &edge
}
