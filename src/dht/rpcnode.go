// A RPC Node type for RPC call

package chord

import (
	"math/big"
	"net"
)

type RPCNode struct {
	O      *Node
	Listen net.Listener
	name   string
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

func (o *RPCNode) FindSuccessor(pos *LookupType, res *Edge) error {
	return o.O.FindSuccessor(pos, res)
}

func (o *RPCNode) Notify(pred *Edge, res *int) error {
	return o.O.Notify(pred, res)
}

func (o *RPCNode) PutValue(kv KVPair, success *bool) error {
	return o.O.PutValue(kv, success)
}

func (o *RPCNode) GetValue(key string, value *string) error {
	return o.O.GetValue(key, value)
}

func (o *RPCNode) DeleteValue(key string, success *bool) error {
	return o.O.DeleteValue(key, success)
}

func (o *RPCNode) PutValueSuccessor(kv KVPair, success *bool) error {
	return o.O.PutValueSuccessor(kv, success)
}

func (o *RPCNode) DeleteValueSuccessor(key string, success *bool) error {
	return o.O.DeleteValueSuccessor(key, success)
}

func (o *RPCNode) PutValueDataPre(kv KVPair, success *bool) error {
	return o.O.PutValueDataPre(kv, success)
}

func (o *RPCNode) DeleteValueDataPre(key string, success *bool) error {
	return o.O.DeleteValueDataPre(key, success)
}

func (o *RPCNode) MoveKVPairs(newNode *big.Int, res *map[string]string) error {
	return o.O.MoveKVPairs(newNode, res)
}

func (o *RPCNode) MoveDataPre(args int, res *map[string]string) error {
	return o.O.MoveDataPre(args, res)
}

func (o *RPCNode) QuitMoveData(Data *KVMap, res *int) error {
	return o.O.QuitMoveData(Data, res)
}

func (o *RPCNode) GetPredecessor(args int, res *Edge) error {
	return o.O.GetPredecessor(args, res)
}

func (o *RPCNode) GetSuccessorList(args int, res *[successorListLen + 1]Edge) error {
	return o.O.GetSuccessorList(args, res)
}

func (o *RPCNode) SetSuccessor(edge Edge, res *int) error {
	return o.O.SetSuccessor(edge, res)
}

func (o *RPCNode) SetPredecessor(edge Edge, res *int) error {
	return o.O.SetPredecessor(edge, res)
}

func (o *RPCNode) AgreeJoin(addr string, agree *bool) error {
	return o.O.AgreeJoin(addr, agree)
}

func (o *RPCNode) PrintMessage(pair StrPair, res *int) error {
	return o.O.PrintMessage(pair, res)
}
