package kademlia

import (
	"net/rpc"
	"time"
)

func (o *Node) RPCPing(p Contact, res *PingReturn) error {
	// Todo: update k-bucket (goroutine?)
	var success bool
	for i := 0; i < 3; i++ {
		chOK := make(chan bool)
		go func() {
			client, err := rpc.Dial("tcp", p.Ip)
			if err == nil {
				err = client.Close()
				chOK <- true
			} else {
				chOK <- false
			}
		}()
		select {
		case ok := <-chOK:
			if ok {
				success = true
			} else {
				continue
			}
		case <-time.After(time.Second / 2):
			break
		}
		if success == true {
			break
		}
	}
	res = &PingReturn{success, p}
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
