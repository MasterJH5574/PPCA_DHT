package kademlia

func (o *Node) RPCPing(p Contact, res *PingReturn) error {
	return nil
}

func (o *Node) RPCStore(obj StoreRequest, res *StoreReturn) error {
	return nil
}

func (o *Node) RPCFindNode(p FindNodeRequest, res *FindNodeReturn) error {
	return nil
}

func (o *Node) RPCFindValue(p FindValueRequest, res *FindValueReturn) error {
	return nil
}
