package main

import (
	chord "dht"
	"log"
	"net/rpc"
	"strconv"
)

func NewNode(port int, name string) client {
	var o client
	o.O = new(chord.RPCNode)
	o.O.O = new(chord.Node)
	o.Port = strconv.Itoa(port)
	o.Name = name
	o.server = rpc.NewServer()
	err := o.server.Register(o.O)
	if err != nil {
		log.Fatalln("Error: Register", err)
	}
	o.O.O.Init(o.Port)

	return o
}
