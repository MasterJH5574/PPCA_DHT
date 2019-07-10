package main

import (
	chord "dht"
	"net/rpc"
	"strconv"
)

func NewNode(port int) *dhtNode {
	var o client
	o.O = new(chord.RPCNode)
	o.O.O = new(chord.Node)
	o.Port = strconv.Itoa(port)
	o.server = rpc.NewServer()
	o.O.O.Init(o.Port)

	var res dhtNode
	res = &o
	return &res
}
