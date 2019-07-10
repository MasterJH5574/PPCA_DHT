// A RPC Node type for RPC call

package chord

import "net"

type RPCNode struct {
	O      *Node
	Listen net.Listener
}

/* method used for rpc call:
    FindSuccessor
    Notify
    GetData
	GetValue
    GetPredecessor
    SetSuccessor
    SetPredecessor
*/

func (o *RPCNode) FindSuccessor(pos *lookupType, res *Edge) error {
	return o.O.FindSuccessor(pos, res)
}

func (o *RPCNode) Notify(pred *Edge, res interface{}) error {
	return o.O.Notify(pred, res)
}

func (o *RPCNode) GetData(args interface{}, res **KVMap) error {
	return o.O.GetData(args, res)
}

func (o *RPCNode) GetValue(key string, value **string) error {
	return o.O.GetValue(key, value)
}

func (o *RPCNode) GetPredecessor(args interface{}, res **Edge) error {
	return o.O.GetPredecessor(args, res)
}

func (o *RPCNode) SetSuccessor(edge Edge, res interface{}) error {
	return o.O.SetSuccessor(edge, res)
}

func (o *RPCNode) SetPredecessor(edge Edge, res interface{}) error {
	return o.O.SetPredecessor(edge, res)
}
