package main

import (
	"dht"
	"fmt"
	"message"
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
	ForceQuit()
	Ping(addr string) bool

	GetAddr() string
	Dump()
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

	/*err := o.server.Register(o.O)
	if err != nil {
		fmt.Println("Error: rpc.Register error: ", err)
		return
	}*/
	//o.server.HandleHTTP()

	listen, err := net.Listen("tcp", ":"+o.Port)
	if err != nil {
		fmt.Println("Error: Listen error: ", err)
		return
	}
	o.O.Listen = listen
	o.O.O.ON = true
	o.O.O.Init(o.Port)
	go o.server.Accept(o.O.Listen)
}

func (o *client) Create() {
	o.O.O.Create()
	go o.O.O.Stabilize(true)
	go o.O.O.FixFingers()
	go o.O.O.CheckPredecessor()

	message.PrintTime()
	fmt.Println("create: success", o.O.O.Addr)
}

func (o *client) Join(addr string) bool {
	res := o.O.O.Join(addr)

	message.PrintTime()
	if res == true {
		go o.O.O.Stabilize(true)
		go o.O.O.FixFingers()
		go o.O.O.CheckPredecessor()
		fmt.Println("join:", o.O.O.Addr, "join a ring containing", addr)
	} else {
		fmt.Println("join: join failure", addr)
	}

	return res
}

func (o *client) Quit() {
	err := o.O.Listen.Close()
	o.O.O.Quit()
	if err != nil {
		fmt.Println("Error: listen close error: ", err)
	}
	o.wg.Add(-1)
}

func (o *client) ForceQuit() {
	o.O.O.ON = false
	err := o.O.Listen.Close()
	if err != nil {
		fmt.Println("Error: listen close error when force quit: ", err)
	}
	fmt.Println("Force quit success")
}

func (o *client) Ping(addr string) bool {
	return o.O.O.Ping(addr)
}

func (o *client) GetAddr() string {
	return o.O.O.Addr
}

func (o *client) Dump() {
	o.O.O.Dump()
}
