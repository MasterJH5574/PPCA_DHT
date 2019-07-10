package main

import (
	"dht"
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type dhtNode interface {
	Get(k string) (bool, string)
	Put(k string, v string) bool
	Del(k string) bool
	Run(wg *sync.WaitGroup)
	Create()
	Join(addr string) bool
	Quit()
	Ping(addr string) bool
}

type client struct {
	O    *chord.RPCNode
	Port string

	wg     *sync.WaitGroup
	server *rpc.Server
	// Listening bool
}

func (o *client) Get(k string) (bool, string) {
	res, success := o.O.O.Get(k)
	return success, res
}

func (o *client) Put(k, v string) bool {
	return o.O.O.Put(k, v)
}

func (o *client) Del(k string) bool {
	return o.O.O.Delete(k)
}

func (o *client) Run(wg *sync.WaitGroup) {
	wg.Add(1)
	o.wg = wg

	err := rpc.Register(*o.O)
	if err != nil {
		fmt.Println("Error: rpc.Register error: ", err)
		return
	}
	rpc.HandleHTTP()

	listen, err := net.Listen("tcp", ":"+o.Port)
	if err != nil {
		fmt.Println("Error: Listen error: ", err)
		return
	}
	o.O.Listen = listen
	o.O.O.Init(o.Port)

	go o.server.Accept(o.O.Listen)
}

func (o *client) Create() {
	o.O.O.Create()
}

func (o *client) Join(addr string) bool {
	return o.O.O.Join(addr)
}

func (o *client) Quit() {
	o.O.O.Quit()
	o.wg.Add(-1)
}

func (o *client) Ping(addr string) bool {
	return o.O.O.Ping(addr)
}
